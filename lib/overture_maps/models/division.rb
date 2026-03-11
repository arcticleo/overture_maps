# frozen_string_literal: true

module OvertureMaps
  module Models
    class Division < Base
      self.table_name = "overture_divisions"

      # Scope: by subtype
      scope :by_subtype, ->(subtype) { where(subtype: subtype) }

      # Scope: by country
      scope :by_country, ->(country) { where(country: country) }

      # Scope: by region
      scope :by_region, ->(region) { where(region: region) }

      # Scope: by admin level
      scope :by_admin_level, ->(level) { where(admin_level: level) }

      # Scope: children of a parent
      scope :children_of, ->(parent_id) {
        where(parent_division_id: parent_id)
      }

      # Scope: top-level (countries)
      scope :countries, -> { where(subtype: "country") }

      # Scope: regions/states
      scope :regions, -> { where(subtype: "region") }

      # Scope: subregions
      scope :subregions, -> { where(subtype: "subregion") }

      # Scope: counties
      scope :counties, -> { where(subtype: "county") }

      # Scope: localities (cities)
      scope :localities, -> { where(subtype: "locality") }

      # Scope: neighborhoods
      scope :neighborhoods, -> { where(subtype: "neighborhood") }

      # Scope: macrohoods
      scope :macrohoods, -> { where(subtype: "macrohood") }

      # Scope: disputed territories
      scope :disputed, -> { where(is_disputed: true) }

      # Scope: territorial
      scope :territorial, -> { where(is_territorial: true) }

      # Scope: land divisions
      scope :land, -> { where(is_land: true) }

      # Scope: has population data
      scope :with_population, -> { where("population IS NOT NULL") }

      # Scope: by population range
      scope :by_population, ->(min: nil, max: nil) {
        query = all
        query = query.where("population >= ?", min) if min
        query = query.where("population <= ?", max) if max
        query
      }

      # Association: parent division
      belongs_to :parent, class_name: "OvertureMaps::Models::Division", optional: true,
                 foreign_key: :parent_division_id, primary_key: :division_id

      # Association: child divisions
      has_many :children, class_name: "OvertureMaps::Models::Division",
               foreign_key: :parent_division_id, primary_key: :division_id

      # Extract primary name
      def display_name
        names.first if names.present?
      end

      # Full path of divisions (country > region > county > locality)
      def ancestry
        parent ? parent.ancestry + [self] : [self]
      end

      # Get capital divisions
      def capitals
        return [] unless capital_division_ids.is_a?(Array) && capital_division_ids.any?

        self.class.where(division_id: capital_division_ids)
      end

      # Check if this is a capital
      def capital?
        capital_of_divisions.is_a?(Array) && capital_of_divisions.any?
      end
    end
  end
end
