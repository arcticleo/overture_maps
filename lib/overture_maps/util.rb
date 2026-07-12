# frozen_string_literal: true

module OvertureMaps
  module Util
    module_function

    def format_size(bytes)
      if bytes >= 1_073_741_824
        "#{(bytes / 1_073_741_824.0).round(2)} GB"
      elsif bytes >= 1_048_576
        "#{(bytes / 1_048_576.0).round(2)} MB"
      elsif bytes >= 1024
        "#{(bytes / 1024.0).round(2)} KB"
      else
        "#{bytes} bytes"
      end
    end

    # Approximate area of a bounding box in km².
    def bbox_area_km2(bbox)
      lat_center = (bbox.min_lat + bbox.max_lat) / 2.0
      km_per_deg_lat = 111.0
      km_per_deg_lng = 111.0 * Math.cos(lat_center * Math::PI / 180.0)

      width_km = (bbox.max_lng - bbox.min_lng) * km_per_deg_lng
      height_km = (bbox.max_lat - bbox.min_lat) * km_per_deg_lat
      (width_km * height_km).abs.round(2)
    end
  end
end
