# frozen_string_literal: true

module OvertureMaps
  # A WGS84 bounding box. The single place that parses user-supplied coordinate
  # strings, so import and download entry points cannot drift apart.
  class BoundingBox
    NUMBER = /-?\d+(?:\.\d+)?/
    # Four numbers separated by commas, whitespace, or underscores, with an
    # optional "|display name" suffix: "47.6,-122.3,47.7,-122.2" or
    # "47.6_-122.3_47.7_-122.2|seattle"
    COORDS = /\A(#{NUMBER})[,\s_]+(#{NUMBER})[,\s_]+(#{NUMBER})[,\s_]+(#{NUMBER})(?:\|(.+))?\z/

    attr_reader :min_lat, :min_lng, :max_lat, :max_lng, :display_name

    def initialize(lat1:, lng1:, lat2:, lng2:, display_name: nil)
      @min_lat, @max_lat = [Float(lat1), Float(lat2)].minmax
      @min_lng, @max_lng = [Float(lng1), Float(lng2)].minmax
      @display_name = display_name

      unless (-90..90).cover?(@min_lat) && (-90..90).cover?(@max_lat) &&
             (-180..180).cover?(@min_lng) && (-180..180).cover?(@max_lng)
        raise ArgumentError, "coordinates out of range: #{self}"
      end
    end

    # Returns a BoundingBox if the string looks like coordinates, else nil.
    def self.parse(string)
      match = COORDS.match(string.to_s.strip)
      return nil unless match

      new(lat1: match[1], lng1: match[2], lat2: match[3], lng2: match[4], display_name: match[5])
    end

    def self.coordinates?(string)
      COORDS.match?(string.to_s.strip)
    end

    # Overture bbox structs come back from division searches as
    # {"xmin" => ..., "xmax" => ..., "ymin" => ..., "ymax" => ...}
    def self.from_overture(bbox, display_name: nil)
      new(
        lat1: bbox["ymin"], lng1: bbox["xmin"],
        lat2: bbox["ymax"], lng2: bbox["xmax"],
        display_name: display_name
      )
    end

    def self.around(lat:, lng:, radius_meters:)
      lat = Float(lat)
      lng = Float(lng)
      lat_delta = radius_meters.to_f / 111_000
      lng_delta = radius_meters.to_f / (111_000 * Math.cos(lat * Math::PI / 180))

      new(
        lat1: [lat - lat_delta, -90].max, lng1: [lng - lng_delta, -180].max,
        lat2: [lat + lat_delta, 90].min, lng2: [lng + lng_delta, 180].min
      )
    end

    def to_s
      "#{min_lat},#{min_lng},#{max_lat},#{max_lng}"
    end

    # Filename-safe label for cache files.
    def slug
      return sanitized_display_name if display_name

      format("%.4f_%.4f_%.4f_%.4f", min_lat, min_lng, max_lat, max_lng)
    end

    private

    def sanitized_display_name
      display_name.downcase.gsub(/[^a-z0-9]+/, "_").gsub(/\A_+|_+\z/, "")
    end
  end
end
