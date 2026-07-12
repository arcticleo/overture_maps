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
    end
  end
end
