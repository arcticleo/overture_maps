# frozen_string_literal: true

module OvertureMaps
  module Models
    # Overture places category taxonomy (no geometry — plain lookup table).
    class Category < ::ActiveRecord::Base
      self.table_name = "overture_categories"

      validates :name, presence: true, uniqueness: true

      scope :by_primary, ->(primary) {
        where(primary_category: primary)
      }

      def self.primary_categories
        distinct.where.not(primary_category: nil).pluck(:primary_category).sort
      end

      # Expands taxonomy groups to the leaf categories beneath them, at any
      # depth ("eat_and_drink" or "restaurant" → every *_restaurant leaf).
      # Names that aren't groups pass through unchanged, so mixed input like
      # ["eat_and_drink", "museum"] works.
      def self.expand(names)
        names = Array(names).map(&:to_s)
        leaves = names.flat_map do |group|
          where("taxonomy @> ?", [group].to_json).pluck(:name)
        end
        (names + leaves).uniq
      end
    end
  end
end
