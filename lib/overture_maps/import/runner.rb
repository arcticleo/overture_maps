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
        attrs = {
          id: record["id"],
          geometry: parse_geometry(record["geometry"]),
          created_at: Time.current,
          updated_at: Time.current
        }

        # Extract names array from struct {primary: "...", common: {...}, rules: [...]}
        if record["names"]
          names = []
          names << record["names"]["primary"] if record["names"]["primary"]
          if record["names"]["common"].is_a?(Hash)
            names.concat(record["names"]["common"].values)
          end
          attrs[:names] = names
        end

        # Categories: store the struct as JSONB
        attrs[:categories] = record["categories"] if record["categories"]

        # Brand: field is "brand" (singular) in Overture data, "brands" in DB
        attrs[:brands] = record["brand"] if record["brand"]

        # Addresses: store the list of address structs as JSONB
        attrs[:addresses] = record["addresses"] if record["addresses"]

        # Extract country from first address if available
        if record["addresses"].is_a?(Array) && record["addresses"].first
          attrs[:country] = record["addresses"].first["country"]
        end

        # Numeric/string fields
        attrs[:confidence] = record["confidence"]&.to_s
        attrs[:elevation] = record["elevation"]

        # Building fields
        attrs[:height] = record["height"]
        attrs[:level] = record["level"]
        attrs[:is_underground] = record["is_underground"]

        # Address fields (for address theme)
        attrs[:street] = record["street"]
        attrs[:locality] = record["locality"]
        attrs[:region] = record["region"]
        attrs[:postcode] = record["postcode"]

        attrs.compact
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
