# frozen_string_literal: true

module OvertureMaps
  module Models
    # All six base-theme feature types (bathymetry, infrastructure, land,
    # land_cover, land_use, water) in one table, discriminated by
    # feature_type.
    class BaseFeature < Base
      self.table_name = "overture_base_features"

      scope :by_feature_type, ->(feature_type) {
        where(feature_type: feature_type)
      }

      scope :by_subtype, ->(subtype) {
        where(subtype: subtype)
      }

      scope :by_class, ->(feature_class) {
        where(feature_class: feature_class)
      }

      scope :water, -> { by_feature_type("water") }
      scope :land, -> { by_feature_type("land") }
      scope :land_use, -> { by_feature_type("land_use") }
      scope :land_cover, -> { by_feature_type("land_cover") }
      scope :infrastructure, -> { by_feature_type("infrastructure") }
      scope :bathymetry, -> { by_feature_type("bathymetry") }
    end
  end
end
