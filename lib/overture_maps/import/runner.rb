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

        # Get valid column names for this model
        valid_columns = model_class.column_names.map(&:to_sym)

        # Deduplicate records by ID (keep last occurrence)
        deduped = records.reverse.uniq { |r| r[:id] || r["id"] }.reverse

        # Filter records to only include valid columns
        normalized = deduped.map do |record|
          record.slice(*valid_columns)
        end

        # Ensure all records have the same keys (upsert_all requires it)
        all_keys = normalized.each_with_object(Set.new) { |r, s| s.merge(r.keys) }.to_a
        normalized.each { |r| all_keys.each { |k| r[k] = nil unless r.key?(k) } }

        # Use upsert_all to handle duplicate keys (update existing records)
        model_class.upsert_all(
          normalized,
          unique_by: :id,
          returning: false,
          record_timestamps: true
        )
        @imported_count += deduped.length
      rescue StandardError => e
        # Batch failed - try individual upserts to identify bad records
        @errors << { error: e.message, records: normalized.length }

        normalized.each do |record|
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

        # Country: direct field in address theme, or extract from nested addresses
        attrs[:country] = record["country"]
        if attrs[:country].nil? && record["addresses"].is_a?(Array) && record["addresses"].first
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

        # Base theme fields
        attrs[:subtype] = record["subtype"]
        attrs[:class] = record["class"]
        attrs[:height] = record["height"]
        attrs[:surface] = record["surface"]
        attrs[:depth] = record["depth"]
        attrs[:level] = record["level"]
        attrs[:is_salt] = record["is_salt"]
        attrs[:is_intermittent] = record["is_intermittent"]
        attrs[:elevation] = record["elevation"]
        attrs[:wikidata] = record["wikidata"]
        attrs[:cartography] = record["cartography"]
        attrs[:source_tags] = record["source_tags"]

        # Division theme fields
        attrs[:division_id] = record["division_id"]
        attrs[:parent_division_id] = record["parent_division_id"]
        attrs[:population] = record["population"]
        attrs[:is_land] = record["is_land"]
        attrs[:is_territorial] = record["is_territorial"]
        attrs[:is_disputed] = record["is_disputed"]
        attrs[:admin_level] = record["admin_level"]
        attrs[:local_type] = record["local_type"]
        attrs[:hierarchies] = record["hierarchies"]
        attrs[:perspectives] = record["perspectives"]
        attrs[:norms] = record["norms"]
        attrs[:capital_division_ids] = record["capital_division_ids"]
        attrs[:capital_of_divisions] = record["capital_of_divisions"]

        # Transportation theme fields
        attrs[:subclass] = record["subclass"]
        attrs[:connectors] = record["connectors"]
        attrs[:routes] = record["routes"]
        attrs[:speed_limits] = record["speed_limits"]
        attrs[:access_restrictions] = record["access_restrictions"]
        attrs[:road_surface] = record["road_surface"]
        attrs[:road_flags] = record["road_flags"]
        attrs[:rail_flags] = record["rail_flags"]
        attrs[:width_rules] = record["width_rules"]
        attrs[:level_rules] = record["level_rules"]
        attrs[:destinations] = record["destinations"]
        attrs[:subclass_rules] = record["subclass_rules"]
        attrs[:prohibited_transitions] = record["prohibited_transitions"]

        attrs.compact
      end

      WKT_PREFIXES = /\A\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*[\(Z]/i

      # Parse geometry from WKT, WKB (binary, hex, or JSON-escaped), or GeoJSON
      # NOTE: RGeo::Error is a Module, not a class. The base exception class is
      # RGeo::Error::RGeoError (inherits RuntimeError). We must rescue that.
      def parse_geometry(geom)
        return nil unless geom

        factory = RGeo::Geographic.spherical_factory(srid: 4326)

        case geom
        when String
          # Try WKT first if it looks like a geometry type name
          if geom.match?(WKT_PREFIXES)
            begin
              return RGeo::WKRep::WKTParser.new(factory).parse(geom)
            rescue RGeo::Error::RGeoError
              # Fall through
            end
          end

          # Check if it's JSON-escaped binary (from DuckDB JSON output)
          # Format: "\x00\x00\x00..." which contains literal backslash-x sequences
          if geom.include?("\\x")
            begin
              hex_pairs = []
              i = 0
              while i < geom.length
                if geom[i] == '\\' && i + 1 < geom.length && geom[i+1] == 'x'
                  if i + 3 < geom.length
                    hex_pairs << geom[i+2..i+3]
                    i += 4
                  else
                    i += 1
                  end
                else
                  hex_pairs << geom[i].ord.to_s(16).rjust(2, '0')
                  i += 1
                end
              end

              hex_string = hex_pairs.join
              return factory.parse_wkb(hex_string)
            rescue RGeo::Error::RGeoError
              # Fall through
            end
          end

          # Check if it's binary WKB (raw bytes from Parquet)
          # WKB starts with endian byte: 0x00 (big-endian) or 0x01 (little-endian)
          if geom.encoding == Encoding::ASCII_8BIT ||
             (geom.bytesize >= 5 && [0, 1].include?(geom.getbyte(0)))
            begin
              return factory.parse_wkb(geom.unpack1("H*"))
            rescue RGeo::Error::RGeoError
              # Fall through
            end
          end

          # Try as hex WKB string
          begin
            return factory.parse_wkb(geom)
          rescue RGeo::Error::RGeoError
            # Fall through
          end

          # Try as WKT
          begin
            return RGeo::WKRep::WKTParser.new(factory).parse(geom)
          rescue RGeo::Error::RGeoError
            # Fall through
          end

          # Try as GeoJSON
          begin
            return RGeo::GeoJSON.decode(geom, geo_factory: factory)
          rescue RGeo::Error::RGeoError, JSON::ParserError
            nil
          end
        when Hash
          begin
            RGeo::GeoJSON.decode(geom, geo_factory: factory)
          rescue RGeo::Error::RGeoError
            nil
          end
        else
          nil
        end
      end
    end
  end
end
