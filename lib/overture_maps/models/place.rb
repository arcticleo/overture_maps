# frozen_string_literal: true

module OvertureMaps
  module Models
    class Place < Base
      self.table_name = "overture_places"

      # Matches the primary category or any alternate. Takes leaf category
      # names from the Overture taxonomy (e.g. "cafe", "restaurant").
      scope :by_category, ->(categories) {
        cats = Array(categories).map(&:to_s)
        where(
          "primary_category = ANY (ARRAY[:cats]::text[]) OR categories->'alternate' ?| ARRAY[:cats]::text[]",
          cats: cats
        )
      }

      scope :by_country, ->(country) {
        where(country: country)
      }

      # Matches the brand's primary name, e.g. by_brand("Starbucks").
      scope :by_brand, ->(brands) {
        where(
          "brands->'names'->>'primary' = ANY (ARRAY[:brands]::text[])",
          brands: Array(brands).map(&:to_s)
        )
      }

      scope :by_operating_status, ->(status) {
        where(operating_status: status)
      }

      scope :min_confidence, ->(confidence) {
        where("confidence >= ?", confidence)
      }

      def brand_name
        brands&.dig("names", "primary")
      end
    end
  end
end
