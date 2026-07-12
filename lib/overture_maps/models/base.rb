# frozen_string_literal: true

require "rgeo"
require "rgeo/geo_json"

module OvertureMaps
  module Models
    class Base < ::ActiveRecord::Base
      self.abstract_class = true

      # Everything whose geometry intersects the bounding box. Works for any
      # geometry type (points, building polygons, segments).
      scope :within_bounds, ->(south, west, north, east) {
        where(
          "ST_Intersects(geometry, ST_MakeEnvelope(?, ?, ?, ?, 4326)::geography)",
          west, south, east, north
        )
      }

      # Within radius_meters of a point.
      scope :near, ->(lat, lng, radius_meters = 1000) {
        where(
          "ST_DWithin(geometry, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, ?)",
          lng, lat, radius_meters
        )
      }

      def to_geojson
        return nil unless geometry

        {
          type: "Feature",
          geometry: RGeo::GeoJSON.encode(geometry),
          properties: attributes.except("geometry", "created_at", "updated_at")
        }
      end
    end
  end
end
