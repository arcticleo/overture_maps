# frozen_string_literal: true

module OvertureMaps
  class BaseModel < ::ActiveRecord::Base
    self.abstract_class = true

    # Override geometry to return a copy to prevent mutation
    def geometry
      super&.clone
    end

    # Convert to GeoJSON
    def to_geojson
      return nil unless geometry

      RGeo::GeoJSON.encode(geometry).to_json
    end

    # Common scope: within bounding box
    scope :within_bounds, ->(south:, west:, north:, east:) {
      where(
        "ST_Contains(ST_MakeEnvelope(?, ?, ?, ?, 4326), geometry)",
        west, south, east, north
      )
    }

    # Common scope: near a point (radial search)
    scope :near, ->(lat:, lng:, radius_meters: 1000) {
      where(
        "ST_DWithin(geometry, ST_Point(?, ?)::geography, ?)",
        lng, lat, radius_meters
      )
    }
  end
end
