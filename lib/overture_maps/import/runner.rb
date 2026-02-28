# frozen_string_literal: true

require "rgeo"

module OvertureMaps
  module Import
    class Runner
      BATCH_SIZE = 1000

      attr_reader :model_class, :batch_size

      def initialize(model_class:, batch_size: BATCH_SIZE)
        @model_class = model_class
        @batch_size = batch_size
        @imported_count = 0
        @error_count = 0
        @errors = []
      end

      # Import from a file path
      def import_from_file(path, transform: nil, filter: nil)
        reader = ParquetReader.new(theme: theme_from_class)

        import_from_reader(reader, source: path, transform: transform, filter: filter)
      end

      # Import from a ParquetReader
      def import_from_reader(reader, source:, transform: nil, filter: nil)
        records = []

        reader.each_record(source: source) do |record|
          # Apply filter if provided
          next if filter && !filter.call(record)

          transformed = transform ? transform.call(record) : record_to_attributes(record)

          if transformed
            records << transformed

            if records.length >= batch_size
              flush_records(records)
              records = []
            end
          end
        end

        # Flush remaining records
        flush_records(records) if records.any?

        self
      end

      # Import from an Enumerable of records
      def import_from_records(records, transform: nil, filter: nil)
        batch = []

        records.each do |record|
          # Apply filter if provided
          next if filter && !filter.call(record)

          transformed = transform ? transform.call(record) : record_to_attributes(record)

          if transformed
            batch << transformed

            if batch.length >= batch_size
              flush_records(batch)
              batch = []
            end
          end
        end

        flush_records(batch) if batch.any?

        self
      end

      def imported_count
        @imported_count
      end

      def error_count
        @error_count
      end

      def errors
        @errors
      end

      def success?
        @error_count.zero?
      end

      private

      def flush_records(records)
        return if records.empty?

        model_class.insert_all!(records)
        @imported_count += records.length
      rescue StandardError => e
        @error_count += records.length
        @errors << { error: e.message, records: records.length }

        # Try inserting one by one to identify bad records
        records.each do |record|
          begin
            model_class.insert!(record)
            @imported_count += 1
            @error_count -= 1
          rescue StandardError => record_error
            @errors << { error: record_error.message, record: record }
          end
        end
      end

      def theme_from_class
        model_class.name.demodulize.underscore
      end

      # Convert Parquet record to model attributes
      def record_to_attributes(record)
        {
          id: record["id"],
          geometry: parse_geometry(record["geometry"]),
          names: record["names"],
          categories: record["categories"],
          brands: record["brands"],
          addresses: record["addresses"],
          confidence: record["confidence"],
          elevation: record["elevation"],
          country: record["country"],
          height: record["height"],
          level: record["level"],
          is_underground: record["is_underground"],
          street: record["street"],
          locality: record["locality"],
          region: record["region"],
          postcode: record["postcode"],
          class: record["class"],
          created_at: Time.current,
          updated_at: Time.current
        }.compact
      end

      # Parse geometry from WKB or GeoJSON
      def parse_geometry(geom)
        return nil unless geom

        factory = RGeo::Geographic.spherical_factory(srid: 4326)

        case geom
        when String
          # Try as WKB hex string
          begin
            factory.parse_wkb(geom)
          rescue RGeo::Error
            # Try as GeoJSON
            RGeo::GeoJSON.decode(geom, geo_factory: factory)
          end
        when Hash
          RGeo::GeoJSON.decode(geom, geo_factory: factory)
        else
          nil
        end
      end
    end
  end
end
