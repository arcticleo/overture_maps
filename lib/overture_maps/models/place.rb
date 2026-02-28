# frozen_string_literal: true

module OvertureMaps
  module Models
    class Place < Base
      self.table_name = "overture_places"

      # Scope: by category
      scope :by_category, ->(categories) {
        where("categories ?| array[:categories]", categories: Array(categories))
      }

      # Scope: by country
      scope :by_country, ->(country) {
        where(country: country)
      }

      # Scope: by brand
      scope :by_brand, ->(brand) {
        where("brands ?| array[:brands]", brands: Array(brand))
      }

      # Parse categories from JSON string or array
      def categories
        read_attribute(:categories).is_a?(String) ? JSON.parse(read_attribute(:categories)) : read_attribute(:categories)
      end

      # Parse brands from JSON string or array
      def brands
        read_attribute(:brands).is_a?(String) ? JSON.parse(read_attribute(:brands)) : read_attribute(:brands)
      end
    end
  end
end
