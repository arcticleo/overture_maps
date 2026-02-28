# frozen_string_literal: true

require "active_record"

module OvertureMaps
  module Database
    class << self
      # Check if PostGIS extension is available
      def postgis_available?
        connection = ActiveRecord::Base.connection
        connection.extension_enabled?("postgis") rescue false
      end

      # Enable PostGIS extension (must be superuser)
      def enable_postgis
        connection = ActiveRecord::Base.connection
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
      end

      # Create a spatial index on a geometry column
      def create_spatial_index(table_name, column_name = "geometry")
        sql = "CREATE INDEX #{table_name}_#{column_name}_idx ON #{table_name} USING GIST (#{column_name})"
        ActiveRecord::Base.connection.execute(sql)
      end

      # Drop a spatial index
      def drop_spatial_index(table_name, column_name = "geometry")
        sql = "DROP INDEX IF EXISTS #{table_name}_#{column_name}_idx"
        ActiveRecord::Base.connection.execute(sql)
      end

      # Rebuild spatial index (for after bulk loads)
      def reindex_spatial(table_name, column_name = "geometry")
        connection = ActiveRecord::Base.connection
        connection.execute("REINDEX INDEX #{table_name}_#{column_name}_idx")
      end

      # Get table geometry info
      def geometry_info(table_name, column_name = "geometry")
        sql = <<~SQL
          SELECT type, srid, has_z, has_m
          FROM geometry_columns
          WHERE f_table_name = '#{table_name}' AND f_geometry_column = '#{column_name}'
        SQL
        ActiveRecord::Base.connection.execute(sql).first
      end

      # Perform a bounding box query
      def bounding_box_query(table_name, south, west, north, east, column_name = "geometry")
        sql = <<~SQL
          SELECT * FROM #{table_name}
          WHERE #{column_name} && ST_MakeEnvelope(#{west}, #{south}, #{east}, #{north}, 4326)
        SQL
        ActiveRecord::Base.connection.execute(sql)
      end

      # Perform a nearest neighbor query
      def nearest_neighbors(table_name, lat, lng, limit = 10, column_name = "geometry")
        sql = <<~SQL
          SELECT *, ST_Distance(#{column_name}, ST_Point(#{lng}, #{lat})::geography) AS distance
          FROM #{table_name}
          ORDER BY #{column_name} <-> ST_Point(#{lng}, #{lat})::geography
          LIMIT #{limit}
        SQL
        ActiveRecord::Base.connection.execute(sql)
      end
    end
  end
end
