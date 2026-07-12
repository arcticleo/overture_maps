# frozen_string_literal: true

module OvertureMaps
  module Models
    class Building < Base
      self.table_name = "overture_buildings"

      scope :by_height, ->(min: nil, max: nil) {
        query = all
        query = query.where("height >= ?", min) if min
        query = query.where("height <= ?", max) if max
        query
      }

      scope :by_floors, ->(min: nil, max: nil) {
        query = all
        query = query.where("num_floors >= ?", min) if min
        query = query.where("num_floors <= ?", max) if max
        query
      }

      scope :by_class, ->(building_class) {
        where(building_class: building_class)
      }

      scope :with_height, -> {
        where("height IS NOT NULL AND height > 0")
      }

      scope :underground, -> {
        where(is_underground: true)
      }
    end
  end
end
