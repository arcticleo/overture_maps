# frozen_string_literal: true

module OvertureMaps
  # GERS — Overture's Global Entity Reference System. IDs are dashed UUIDs
  # (since June 2025); earlier releases used 32-char undashed hex.
  module GERS
    UUID_FORMAT = /\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/
    LEGACY_FORMAT = /\A\h{32}\z/

    class << self
      def valid_id?(id)
        id.is_a?(String) && (id.match?(UUID_FORMAT) || id.match?(LEGACY_FORMAT))
      end

      # Looks an id up in the official registry (the unversioned source of
      # truth for all published GERS ids). Returns a hash with keys like
      # "version", "first_seen", "last_seen", "last_changed", "path", "bbox",
      # or nil when unknown. Scans registry parquet on S3 — takes seconds.
      def lookup(id)
        raise ArgumentError, "not a GERS id: #{id.inspect}" unless valid_id?(id)

        source = "#{OvertureMaps.configuration.s3_uri.chomp("/")}/registry/*.parquet"
        rows = QueryEngine.instance.query(
          "SELECT * FROM read_parquet('#{source}') WHERE id = ? LIMIT 1", [id]
        )
        rows.first
      end
    end
  end
end
