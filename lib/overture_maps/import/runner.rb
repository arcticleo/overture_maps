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

        # Deduplicate records by ID (keep last occurrence)
        deduped = records.reverse.uniq { |r| r[:id] || r["id"] }.reverse
        duplicates_count = records.length - deduped.length

        # Normalize records to all have the same keys (required for upsert_all)
        all_keys = deduped.flat_map(&:keys).uniq
        normalized = deduped.map do |record|
          all_keys.each_with_object({}) do |key, hash|
            hash[key] = record.fetch(key, nil)
          end
        end

        # Use upsert_all to handle duplicate keys (update existing records)
        model_class.upsert_all(
          normalized,
          unique_by: :id,
          returning: false,
          record_timestamps: true
        )
        @imported_count += deduped.length
      rescue StandardError => e
        # Batch failed - will try individual upserts
        @errors << { error: e.message, records: deduped.length }

        # Try upserting one by one to identify bad records
        deduped.each do |record|
          begin
            model_class.upsert(
              record,
              unique_by: :id,
              returning: false,
              record_timestamps: true
            )
            @imported_count += 1
          rescue StandardError => record_error
            @error_count += 1
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
        attrs[:number] = record["number"]
        attrs[:unit] = record["unit"]
        attrs[:postal_city] = record["postal_city"]
        attrs[:postcode] = record["postcode"]
        attrs[:address_levels] = record["address_levels"]

        attrs.compact
      end

      # Parse geometry from WKB (binary, hex, or JSON-escaped), WKT, or GeoJSON
      def parse_geometry(geom)
        return nil unless geom

        factory = RGeo::Geographic.spherical_factory(srid: 4326)

        case geom
        when String
          # Check if it's JSON-escaped binary (from DuckDB JSON output)
          # Format: "\x00\x00\x00..." which contains literal backslash-x sequences
          if geom.include?("\\x")
            begin
              # Parse the escape sequences: \xNN becomes actual byte with value NN
              hex_pairs = []
              i = 0
              while i < geom.length
                if geom[i] == '\\' && i + 1 < geom.length && geom[i+1] == 'x'
                  # This is \x - the next two chars are hex
                  if i + 3 < geom.length
                    hex_pairs << geom[i+2..i+3]
                    i += 4
                  else
                    i += 1
                  end
                else
                  # Regular char - convert to hex
                  hex_pairs << geom[i].ord.to_s(16).rjust(2, '0')
                  i += 1
                end
              end

              hex_string = hex_pairs.join
              return factory.parse_wkb(hex_string)
            rescue RGeo::Error
              # Fall through to other formats
            end
          end

          # Check if it's binary WKB (starts with 01 or 00 for endian marker)
          if geom.bytesize >= 5 && geom[0..1].match?(/\A[01]\x00/)
            begin
              return factory.parse_wkb(geom.unpack1("H*"))
            rescue RGeo::Error
              # Fall through
            end
          end

          # Try as hex WKB string
          begin
            return factory.parse_wkb(geom)
          rescue RGeo::Error
            # Fall through
          end

          # Try as WKT
          begin
            return RGeo::WKRep::WKTParser.new(factory).parse(geom)
          rescue RGeo::Error
            # Fall through
          end

          # Try as GeoJSON
          begin
            return RGeo::GeoJSON.decode(geom, geo_factory: factory)
          rescue RGeo::Error
            nil
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
