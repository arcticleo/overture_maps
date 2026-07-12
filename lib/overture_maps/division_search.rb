# frozen_string_literal: true

module OvertureMaps
  # Resolves location names to division areas. Prefers the locally imported
  # overture_divisions table (fast, offline); falls back to querying the
  # divisions theme on Overture's bucket. Both paths return the same shape:
  # { id:, name:, subtype:, country:, region:, bbox: BoundingBox, area_km2: }
  # ordered largest-area first.
  module DivisionSearch
    class << self
      def search(query:, release: nil, limit: 20)
        local(query: query, limit: limit) ||
          Import::Downloader.search_divisions(query: query, release: release, limit: limit)
      end

      # Returns nil (fall back to remote) when the table is missing, empty,
      # unreachable, or has no match for the query.
      def local(query:, limit: 20)
        model = Models::Division
        return nil unless model.table_exists?

        rows = model.search_by_name(query).largest_first.limit(limit)
        return nil if rows.empty?

        rows.filter_map do |division|
          bbox = division.to_bounding_box
          next unless bbox

          {
            id: division.id,
            name: division.name,
            subtype: division.subtype,
            country: division.country,
            region: division.region,
            bbox: bbox,
            area_km2: Util.bbox_area_km2(bbox)
          }
        end.then { |results| results.empty? ? nil : results }
      rescue StandardError
        nil
      end
    end
  end
end
