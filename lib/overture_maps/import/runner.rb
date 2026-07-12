# frozen_string_literal: true

require "rgeo"
require "rgeo/geo_json"
require "json"

module OvertureMaps
  module Import
    # Batches records into idempotent upserts. Re-running an import updates
    # existing rows (keyed on the GERS id primary key) instead of failing.
    class Runner
      MAX_STORED_ERRORS = 50
      PROGRESS_INTERVAL = 10_000

      attr_reader :model_class, :theme, :type, :batch_size, :imported_count, :error_count, :errors

      def initialize(model_class:, theme: nil, type: nil, batch_size: nil, mapper: nil,
                     release: nil, progress_every: PROGRESS_INTERVAL)
        @model_class = model_class
        @theme = theme
        @type = type
        @release = release
        @batch_size = batch_size || OvertureMaps.configuration.batch_size
        @mapper = mapper
        @progress_every = progress_every
        @imported_count = 0
        @error_count = 0
        @errors = []
      end

      # Imports from anything that yields raw Overture record hashes.
      def import_from_records(records, transform: nil, filter: nil)
        batch = []

        records.each do |record|
          next if filter && !filter.call(record)

          attrs = transform_record(record, transform)
          next unless attrs

          batch << attrs
          if batch.length >= batch_size
            flush_records(batch)
            batch = []
          end
        end

        flush_records(batch) if batch.any?
        self
      end

      def import_from_reader(reader, source:, transform: nil, filter: nil)
        import_from_records(reader.enum_for(:each_record, source: source), transform: transform, filter: filter)
      end

      def import_from_file(path, theme: nil, transform: nil, filter: nil)
        reader = ParquetReader.new(theme: theme)
        import_from_reader(reader, source: path, transform: transform, filter: filter)
      end

      def success?
        @error_count.zero?
      end

      private

      def transform_record(record, transform)
        attrs = transform ? transform.call(record) : default_mapper.call(record)
        return nil unless attrs

        if attrs[:geometry] || attrs["geometry"]
          key = attrs.key?(:geometry) ? :geometry : "geometry"
          attrs[key] = parse_geometry(attrs[key])
        end
        attrs
      rescue StandardError => e
        record_error("mapping failed: #{e.message}", record["id"])
        nil
      end

      def default_mapper
        @default_mapper ||= @mapper || RecordMapper.for(
          theme: theme, type: type, model_class: model_class, release: @release
        )
      end

      def flush_records(records)
        return if records.empty?

        model_class.upsert_all(records, unique_by: model_class.primary_key)
        @imported_count += records.length
        log_progress(records.length)
      rescue StandardError
        # Isolate bad rows without abandoning the batch.
        records.each do |record|
          model_class.upsert_all([record], unique_by: model_class.primary_key)
          @imported_count += 1
        rescue StandardError => e
          record_error(e.message, record[:id] || record["id"])
        end
      end

      def record_error(message, record_id = nil)
        @error_count += 1
        @errors << { error: message, record_id: record_id } if @errors.length < MAX_STORED_ERRORS
      end

      # A line every @progress_every rows so long imports show life.
      def log_progress(batch_length)
        return unless @progress_every&.positive?
        return unless (@imported_count / @progress_every) > ((@imported_count - batch_length) / @progress_every)

        logger = OvertureMaps.configuration.logger
        message = "  #{@imported_count} rows..."
        logger ? logger.info(message) : puts(message)
      end

      # A geometry that fails to parse skips that record — it must never
      # abort the import loop (see transform_record's rescue).
      def parse_geometry(geom)
        GeometryParser.parse(geom)
      end
    end
  end
end
