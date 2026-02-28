# frozen_string_literal: true

module OvertureMaps
  module Models
    class Building < Base
      self.table_name = "overture_buildings"

      # Scope: by height range
      scope :by_height, ->(min: nil, max: nil) {
        query = all
        query = query.where("height >= ?", min) if min
        query = query.where("height <= ?", max) if max
        query
      }

      # Scope: by level range
      scope :by_level, ->(min: nil, max: nil) {
        query = all
        query = query.where("level >= ?", min) if min
        query = query.where("level <= ?", max) if max
        query
      }

      # Scope: has height data
      scope :with_height, -> {
        where("height IS NOT NULL AND height > 0")
      }
    end
  end
end
