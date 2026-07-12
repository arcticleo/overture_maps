# frozen_string_literal: true

module OvertureMaps
  # PostGIS helpers. Identifiers are quoted and values sanitized — nothing
  # user-supplied is interpolated into SQL.
  module Database
    class << self
      def postgis_available?
        connection.extension_enabled?("postgis")
      rescue StandardError
        false
      end

      def enable_postgis
        connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
      end

      def create_spatial_index(table_name, column_name = "geometry")
        index = quote_ident("#{table_name}_#{column_name}_idx")
        connection.execute(
          "CREATE INDEX IF NOT EXISTS #{index} ON #{quote_ident(table_name)} " \
          "USING GIST (#{quote_ident(column_name)})"
        )
      end

      def drop_spatial_index(table_name, column_name = "geometry")
        connection.execute("DROP INDEX IF EXISTS #{quote_ident("#{table_name}_#{column_name}_idx")}")
      end

      def reindex_spatial(table_name, column_name = "geometry")
        connection.execute("REINDEX INDEX #{quote_ident("#{table_name}_#{column_name}_idx")}")
      end

      def geometry_info(table_name, column_name = "geometry")
        exec_sanitized(
          "SELECT type, srid FROM geometry_columns WHERE f_table_name = ? AND f_geometry_column = ?",
          table_name.to_s, column_name.to_s
        ).first
      end

      def bounding_box_query(table_name, south, west, north, east, column_name = "geometry")
        exec_sanitized(
          "SELECT * FROM #{quote_ident(table_name)} " \
          "WHERE #{quote_ident(column_name)} && ST_MakeEnvelope(?, ?, ?, ?, 4326)::geography",
          Float(west), Float(south), Float(east), Float(north)
        )
      end

      def nearest_neighbors(table_name, lat, lng, limit = 10, column_name = "geometry")
        column = quote_ident(column_name)
        exec_sanitized(
          "SELECT *, ST_Distance(#{column}, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography) AS distance " \
          "FROM #{quote_ident(table_name)} " \
          "ORDER BY #{column} <-> ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography LIMIT ?",
          Float(lng), Float(lat), Float(lng), Float(lat), Integer(limit)
        )
      end

      private

      def exec_sanitized(sql, *values)
        connection.exec_query(ActiveRecord::Base.sanitize_sql([sql, *values]))
      end

      def connection
        ActiveRecord::Base.connection
      end

      def quote_ident(name)
        connection.quote_table_name(name.to_s)
      end
    end
  end
end
