# frozen_string_literal: true

module OvertureMaps
  module Import
    class Error < StandardError; end
    class ParquetError < Error; end

    # Base importer class
    class BaseImporter
      attr_reader :theme, :options

      def initialize(theme, options = {})
        @theme = theme
        @options = options
      end

      def import
        raise NotImplementedError
      end

      protected

      def parquet_reader
        @parquet_reader ||= Parquet::ParquetReader.new(parquet_file)
      rescue ParquetError => e
        raise ParquetError, "Failed to open Parquet file: #{e.message}"
      end

      def parquet_file
        @options[:file] || default_parquet_path
      end

      def default_parquet_path
        "s3://overturemaps-us-west-2/release/#{theme}/#{version}/*.parquet"
      end

      def version
        @options[:version] || "2025-01-15"
      end

      def batch_size
        @options[:batch_size] || 1000
      end

      def model_class
        @options[:model_class] || "Overture#{theme.to_s.classify}".constantize
      end
    end
  end
end
