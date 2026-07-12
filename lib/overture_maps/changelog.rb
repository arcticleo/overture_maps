# frozen_string_literal: true

module OvertureMaps
  # Overture's per-release data changelog: parquet partitioned by theme,
  # type, and change_type (added | removed | data_changed | unchanged),
  # with each row carrying the feature id and its bbox. The changelog for
  # release R describes changes relative to the release before R.
  module Changelog
    CHANGE_TYPES = %w[added removed data_changed unchanged].freeze

    class << self
      # IDs removed in `release` (relative to the prior release), optionally
      # restricted to those whose bbox intersects the given area.
      def removed_ids(theme:, type:, release:, bbox: nil)
        sql, params = build_query(theme: theme, type: type, release: release,
                                  change_type: "removed", bbox: bbox, select: "id")
        QueryEngine.instance.query(sql, params).map { |row| row["id"] }
      end

      # Per-change_type counts for a release/theme/type (for sync:status and
      # dry runs). Returns e.g. {"added" => 123, "removed" => 4, ...}.
      def counts(theme:, type:, release:, bbox: nil)
        validate!(theme, type)
        source = source_glob(theme: theme, type: type, release: Releases.validate!(release),
                             change_type: "*")
        sql = +"SELECT change_type, count(*) AS n FROM read_parquet('#{source}', hive_partitioning=1)"
        params = []
        if bbox
          sql << " WHERE bbox.xmin <= ? AND bbox.xmax >= ? AND bbox.ymin <= ? AND bbox.ymax >= ?"
          params = bbox_params(bbox)
        end
        sql << " GROUP BY change_type"

        QueryEngine.instance.query(sql, params)
                   .to_h { |row| [row["change_type"], Integer(row["n"])] }
      end

      private

      def build_query(theme:, type:, release:, change_type:, bbox:, select:)
        validate!(theme, type)
        raise ArgumentError, "unknown change_type: #{change_type}" unless CHANGE_TYPES.include?(change_type)

        source = source_glob(theme: theme, type: type, release: Releases.validate!(release),
                             change_type: change_type)
        sql = +"SELECT #{select} FROM read_parquet('#{source}', hive_partitioning=1)"
        params = []
        if bbox
          sql << " WHERE bbox.xmin <= ? AND bbox.xmax >= ? AND bbox.ymin <= ? AND bbox.ymax >= ?"
          params = bbox_params(bbox)
        end
        [sql, params]
      end

      def bbox_params(bbox)
        [bbox.max_lng, bbox.min_lng, bbox.max_lat, bbox.min_lat]
      end

      def source_glob(theme:, type:, release:, change_type:)
        base = OvertureMaps.configuration.s3_uri.chomp("/")
        "#{base}/changelog/#{release}/theme=#{theme}/type=#{type}/change_type=#{change_type}/*.parquet"
      end

      def validate!(theme, type)
        types = Import::Downloader::TYPES[theme] or raise ArgumentError, "unknown theme: #{theme}"
        raise ArgumentError, "unknown type #{type} for #{theme}" unless types.include?(type)
      end
    end
  end
end
