# frozen_string_literal: true

module OvertureMaps
  module Models
    class Division < Base
      self.table_name = "overture_divisions"

      # Scope: by type
      scope :by_type, ->(type) {
        where(division_type: type)
      }

      # Scope: by ISO code
      scope :by_iso_code, ->(iso_code) {
        where(iso_code: iso_code)
      }

      # Scope: children of a parent
      scope :children_of, ->(parent_id) {
        where(parent_id: parent_id)
      }

      # Scope: top-level (no parent)
      scope :top_level, -> {
        where(parent_id: nil)
      }
    end
  end
end
