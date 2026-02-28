# frozen_string_literal: true

require "rgeo"

module OvertureMaps
  module Import
    class Importer
      FACTORY = RGeo::Geographic.spherical_factory(srid: 4326)

      attr_reader :model_class, :batch_size, :theme

      def initialize(model_class:, batch_size: 1000)
        @model_class = model_class
        @batch_size = batch_size
        @theme = theme_from_class(model_class)
      end

      # Import records from a ParquetReader
      def import_from_reader(reader, source: :s3, &block)
        records = []
        count = 0

        reader.each_record(source: source) do |record|
          transformed = transform_record(record)
          records << transformed
          count += 1

          if records.size >= batch_size
            insert_records(records)
            yield count if block_given?
            records = []
          end
        end

        # Insert remaining records
        insert_records(records) if records.any?
        yield count if block_given?

        count
      end

      # Import from local Parquet file
      def import_from_file(file_path, &block)
        reader = ParquetReader.new(theme: theme)
        import_from_reader(reader, source: file_path, &block)
      end

      private

      def theme_from_class(model_class)
        model_class.table_name.gsub("overture_", "")
      end

      def insert_records(records)
        return if records.empty?

        # Use activerecord-import for bulk insert if available
        if defined?(ActiveRecord::Import)
          model_class.import records, validate: false
        else
          records.each do |record|
            model_class.create!(record)
          end
        end
      end

      def transform_record(record)
        transformed = record.dup

        # Convert geometry if present
        if transformed["geometry"]
          transformed["geometry"] = parse_geometry(transformed["geometry"])
        end

        transformed
      end

      def parse_geometry(geom_data)
        case geom_data
        when Hash
          # GeoJSON format
          parse_geojson(geom_data)
        when String
          # WKT format
          FACTORY.parse_wkt(geom_data)
        else
          nil
        end
      end

      def parse_geojson(geojson)
        case geojson["type"]
        when "Point"
          FACTORY.point(geojson["coordinates"][0], geojson["coordinates"][1])
        when "LineString"
          FACTORY.line_string(geojson["coordinates"].map { |c| FACTORY.point(c[0], c[1]) })
        when "Polygon"
          FACTORY.polygon(geojson["coordinates"][0].map { |c| FACTORY.point(c[0], c[1]) })
        when "MultiLineString"
          FACTORY.multi_line_string(
            geojson["coordinates"].map do |line|
              FACTORY.line_string(line.map { |c| FACTORY.point(c[0], c[1]) })
            end
          )
        when "MultiPolygon"
          FACTORY.multi_polygon(
            geojson["coordinates"].map do |polygon|
              FACTORY.polygon(polygon[0].map { |c| FACTORY.point(c[0], c[1]) })
            end
          )
        else
          nil
        end
      end
    end
  end
end
