# frozen_string_literal: true

module OvertureMaps
  module Models
    class BaseFeature < Base
      self.table_name = "overture_base_features"

      # Scope: by subtype
      scope :by_subtype, ->(subtype) { where(subtype: subtype) }

      # Scope: by class
      scope :by_class, ->(klass) { where(class: klass) }

      # Scope: water features
      scope :water, -> { where(subtype: "water") }

      # Scope: land features
      scope :land, -> { where(subtype: "land") }

      # Scope: land_use features
      scope :land_use, -> { where(subtype: "land_use") }

      # Scope: infrastructure
      scope :infrastructure, -> { where(subtype: "infrastructure") }

      # Scope: bathymetry
      scope :bathymetry, -> { where(subtype: "bathymetry") }

      # Scope: land_cover
      scope :land_cover, -> { where(subtype: "land_cover") }

      # Scope: by height range
      scope :by_height, ->(min: nil, max: nil) {
        query = all
        query = query.where("height >= ?", min) if min
        query = query.where("height <= ?", max) if max
        query
      }

      # Scope: salt water
      scope :salt_water, -> { where(is_salt: true) }

      # Scope: intermittent
      scope :intermittent, -> { where(is_intermittent: true) }

      # Extract primary name
      def display_name
        names.first if names.present?
      end
    end
  end
end
