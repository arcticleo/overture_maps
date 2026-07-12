# frozen_string_literal: true

module OvertureMaps
  module Models
    # Table and importer arrive in Phase 2; the class is defined so app code
    # can reference it once divisions are imported.
    class Division < Base
      self.table_name = "overture_divisions"

      scope :by_subtype, ->(subtype) {
        where(subtype: subtype)
      }

      scope :by_country, ->(country) {
        where(country: country)
      }

      scope :children_of, ->(parent_division_id) {
        where(parent_division_id: parent_division_id)
      }

      scope :countries, -> {
        where(subtype: "country")
      }
    end
  end
end
