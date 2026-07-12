# frozen_string_literal: true

module OvertureMaps
  module Models
    # Imported division areas (theme=divisions/type=division_area): the
    # geocodable territories of countries, regions, counties, localities, etc.
    # Once populated, location searches resolve locally instead of querying
    # Overture's bucket.
    class Division < Base
      self.table_name = "overture_divisions"

      scope :by_subtype, ->(subtype) {
        where(subtype: subtype)
      }

      scope :by_country, ->(country) {
        where(country: country)
      }

      scope :countries, -> {
        where(subtype: "country")
      }

      scope :search_by_name, ->(query) {
        where("name ILIKE ?", "%#{sanitize_sql_like(query)}%")
      }

      scope :largest_first, -> {
        order(Arel.sql("(bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin) DESC"))
      }

      def to_bounding_box
        return nil unless bbox_xmin && bbox_xmax && bbox_ymin && bbox_ymax

        BoundingBox.new(
          lat1: bbox_ymin, lng1: bbox_xmin,
          lat2: bbox_ymax, lng2: bbox_xmax,
          display_name: name
        )
      end
    end
  end
end
