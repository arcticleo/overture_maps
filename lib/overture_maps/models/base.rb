# frozen_string_literal: true

require "rgeo/active_record"

module OvertureMaps
  module Models
    class Base < ::ActiveRecord::Base
      self.abstract_class = true

      # Scope: within bounding box
      scope :within_bounds, ->(south, west, north, east) {
        where(
          "ST_X(geometry::geometry) BETWEEN ? AND ? AND ST_Y(geometry::geometry) BETWEEN ? AND ?",
          west, east, south, north
        )
      }

      # Scope: near a point (radial query)
      scope :near, ->(lat, lng, radius_meters = 1000) {
        where(
          "ST_DWithin(geometry, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, ?)",
          lng, lat, radius_meters
        )
      }

      # Convert to GeoJSON
      def to_geojson
        return nil unless geometry

        {
          type: "Feature",
          geometry: RGeo::GeoJSON.encode(geometry),
          properties: attributes.except("id", "geometry", "created_at", "updated_at")
        }
      end
    end
  end
end
