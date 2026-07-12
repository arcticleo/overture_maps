# frozen_string_literal: true

require "json"

module OvertureMaps
  # Discovers available Overture releases. Primary source is the official
  # STAC catalog; falls back to listing the bucket's release/ prefixes.
  # Results are memoized per process.
  module Releases
    STAC_CATALOG_URL = "https://stac.overturemaps.org/catalog.json"
    RELEASE_FORMAT = /\A\d{4}-\d{2}-\d{2}(?:\.\d+)?\z/
    RELEASE_PATTERN = /\d{4}-\d{2}-\d{2}(?:\.\d+)?/

    class Error < OvertureMaps::Error; end

    class << self
      # All known releases, newest first.
      def all
        @all ||= (from_stac || from_bucket).sort.reverse.freeze
      end

      def latest
        all.first or raise Error, "no Overture releases found"
      end

      # The release to use: explicit config wins, otherwise latest.
      def current
        configured = OvertureMaps.configuration.release
        return validate!(configured) if configured

        latest
      end

      def validate!(release)
        unless release.to_s.match?(RELEASE_FORMAT)
          raise Error, "invalid release #{release.inspect} (expected e.g. 2026-06-17.0)"
        end

        release.to_s
      end

      def reset!
        @all = nil
      end

      private

      def from_stac
        require "overture_maps/storage"

        body = Storage.get(STAC_CATALOG_URL)
        catalog = JSON.parse(body)
        releases = Array(catalog["links"])
                   .select { |link| link["rel"] == "child" }
                   .filter_map { |link| link["href"].to_s[RELEASE_PATTERN] }
                   .uniq
        releases.empty? ? nil : releases
      rescue StandardError => e
        OvertureMaps.configuration.logger&.warn("STAC catalog unavailable (#{e.message}); listing bucket instead")
        nil
      end

      def from_bucket
        require "overture_maps/storage"

        listing = Storage.list(prefix: "release/", delimiter: "/")
        releases = listing[:prefixes].filter_map { |p| p[RELEASE_PATTERN] }.uniq
        raise Error, "could not discover Overture releases from STAC or the bucket" if releases.empty?

        releases
      end
    end
  end
end
