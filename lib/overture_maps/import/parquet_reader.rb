# frozen_string_literal: true

require "parquet"

module OvertureMaps
  module Import
    class ParquetReader
      THEMES = %w[addresses buildings base divisions places transportation].freeze

      attr_reader :theme, :region, :version

      def initialize(theme:, region: nil, version: "2025-01-17")
        @theme = theme
        @region = region
        @version = version
      end

      # Get the S3 URI for this theme/region
      def s3_uri
        base = "s3://overturemaps-us-west-2/release"
        if region
          "#{base}/#{theme}/#{region}/#{theme}-#{version}.parquet"
        else
          "#{base}/#{theme}/#{theme}-#{version}.parquet"
        end
      end

      # Get the Azure URI for this theme/region
      def azure_uri
        base = "wasbs://release@overturemapswestus2.blob.core.windows.net/release"
        if region
          "#{base}/#{theme}/#{region}/#{theme}-#{version}.parquet"
        else
          "#{base}/#{theme}/#{theme}-#{version}.parquet"
        end
      end

      # Iterate over records in the Parquet file
      def each_record(source:, &block)
        raise ArgumentError, "Source must be a file path" unless source.is_a?(String)

        Parquet.each_row(source, &block)
      end

      # Get record count without loading all data
      def record_count(source:)
        raise ArgumentError, "Source must be a file path" unless source.is_a?(String)

        Parquet.metadata(source)["num_rows"]
      end

      # List available regions for a theme (requires AWS SDK)
      def self.list_regions(theme:, version: "2025-01-17")
        require "aws-sdk-s3"

        s3 = Aws::S3Client.new(region: "us-west-2")
        prefix = "release/#{theme}/"

        objects = s3.list_objects_v2(bucket: "overturemaps-us-west-2", prefix: prefix)
        objects.contents.map { |o| o.key.split("/")[1] }.compact.uniq
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      # List available versions (requires AWS SDK)
      def self.list_versions
        require "aws-sdk-s3"

        s3 = Aws::S3Client.new(region: "us-west-2")
        objects = s3.list_objects_v2(bucket: "overturemaps-us-west-2", prefix: "release/")

        objects.common_prefixes.map { |o| o.prefix.split("/")[1] }.compact.uniq.sort.reverse
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      # Check if a local file exists for a theme + location combination
      # @param theme [String] The theme name (e.g., "places", "buildings")
      # @param location [String, nil] The location name (e.g., "Seattle", "California") or nil for any
      # @param output_dir [String] Directory to search (default: tmp/overture)
      # @return [String, nil] Path to the matching file, or nil if not found
      def self.find_local_file(theme:, location: nil, output_dir: "tmp/overture")
        return nil unless Dir.exist?(output_dir)

        pattern = File.join(output_dir, "#{theme}_*.parquet")
        files = Dir.glob(pattern)

        if location
          # Try to find a file matching the location name
          normalized_location = location.to_s.downcase.gsub(/\s+/, "_")

          # First try exact match
          exact_match = files.find { |f| f.downcase.include?(normalized_location) }
          return exact_match if exact_match
        end

        # Return the most recently modified file if any exist
        files.sort_by { |f| File.mtime(f) }.last
      end

      # Query S3 directly using DuckDB with spatial filtering
      # Returns records as an array of hashes
      def self.query_s3_with_bbox(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng, version: nil)
        require "tempfile"
        require "open3"
        require "json"

        Downloader.ensure_duckdb_cli!

        columns = case theme
        when "places"
          "*"
        when "buildings"
          "id, names, height, level, class, is_underground, geometry"
        when "addresses"
          "*"
        when "divisions"
          "*"
        when "base"
          "*"
        when "transportation"
          "*"
        else
          "*"
        end

        sql = <<~SQL.squish
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          SELECT #{columns}
          FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=#{theme}/type=#{type}/*', union_by_name=true)
          WHERE bbox.xmin > #{min_lng}
            AND bbox.xmax < #{max_lng}
            AND bbox.ymin > #{min_lat}
            AND bbox.ymax < #{max_lat}
        SQL

        Downloader.run_duckdb_sql(sql)
      end

      # Stream records from S3 with spatial filtering using DuckDB
      # Yields each record for memory-efficient processing
      def self.stream_s3_with_bbox(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng, version: nil, &block)
        records = query_s3_with_bbox(
          theme: theme,
          type: type,
          min_lat: min_lat,
          max_lat: max_lat,
          min_lng: min_lng,
          max_lng: max_lng,
          version: version
        )

        records.each(&block)
      end
    end
  end
end
