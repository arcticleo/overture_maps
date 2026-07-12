# frozen_string_literal: true

module OvertureMaps
  module Models
    # Transportation segments: center-lines of roads, rails, and waterways.
    class Segment < Base
      self.table_name = "overture_segments"

      scope :by_subtype, ->(subtype) {
        where(subtype: subtype)
      }

      scope :by_class, ->(segment_class) {
        where(segment_class: segment_class)
      }

      scope :roads, -> { where(subtype: "road") }
      scope :rails, -> { where(subtype: "rail") }
      scope :waterways, -> { where(subtype: "water") }
    end
  end
end
