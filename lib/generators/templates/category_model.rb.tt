# frozen_string_literal: true

class OvertureCategory < ApplicationRecord
  self.table_name = "overture_categories"

  validates :name, presence: true, uniqueness: true

  # Scope: by primary category
  scope :by_primary, ->(primary) {
    where(primary_category: primary)
  }

  # Get all unique primary categories
  def self.primary_categories
    distinct.where.not(primary_category: nil).pluck(:primary_category).sort
  end
end
