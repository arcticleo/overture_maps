# frozen_string_literal: true

module OvertureMaps
  module Models
    # Bookkeeping for every area/type combination that has been imported:
    # which release the rows came from and the bbox they cover. This is what
    # overture_maps:sync iterates to bring tables up to a newer release.
    class ImportedArea < ::ActiveRecord::Base
      self.table_name = "overture_imported_areas"

      validates :theme, :feature_type, :model_class_name, :slug, :release, presence: true

      scope :for_release, ->(release) { where(release: release) }
      scope :behind, ->(release) { where.not(release: release) }

      def to_bounding_box
        BoundingBox.new(
          lat1: bbox_ymin, lng1: bbox_xmin,
          lat2: bbox_ymax, lng2: bbox_xmax,
          display_name: slug
        )
      end

      def model_class
        model_class_name.constantize
      end

      # Upserts the bookkeeping row after an import.
      def self.record!(theme:, feature_type:, model_class:, bbox:, release:, records_count:)
        area = find_or_initialize_by(theme: theme, feature_type: feature_type, slug: bbox.slug)
        area.update!(
          model_class_name: model_class.name,
          bbox_xmin: bbox.min_lng, bbox_xmax: bbox.max_lng,
          bbox_ymin: bbox.min_lat, bbox_ymax: bbox.max_lat,
          release: release,
          records_count: records_count
        )
        area
      end
    end
  end
end
