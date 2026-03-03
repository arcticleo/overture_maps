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
    end
  end
end
