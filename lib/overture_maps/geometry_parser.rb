# frozen_string_literal: true

require "rgeo"
require "rgeo/geo_json"

module OvertureMaps
  # Parses the geometry encodings Overture data shows up in: WKB (binary or
  # hex, from parquet), WKT (from DuckDB text output), and GeoJSON (hash or
  # string). Returns RGeo features on the spherical WGS84 factory.
  module GeometryParser
    module_function

    def factory
      @factory ||= RGeo::Geographic.spherical_factory(srid: 4326)
    end

    def parse(geom)
      return nil if geom.nil?
      return geom if geom.is_a?(RGeo::Feature::Instance)

      case geom
      when Hash
        RGeo::GeoJSON.decode(geom, geo_factory: factory)
      when String
        parse_string(geom)
      end
    end

    def parse_string(geom)
      stripped = geom.strip
      if stripped.start_with?("{")
        RGeo::GeoJSON.decode(stripped, geo_factory: factory)
      elsif stripped.match?(/\A[A-Za-z]/)
        factory.parse_wkt(stripped)
      else
        # Binary or hex WKB; RGeo's parser auto-detects hex strings.
        factory.parse_wkb(geom)
      end
    end
  end
end
