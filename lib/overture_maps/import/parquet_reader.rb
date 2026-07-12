# frozen_string_literal: true

require "parquet"

module OvertureMaps
  module Import
    # Reads local Overture parquet files row by row. Remote access and
    # spatial filtering live in Downloader/QueryEngine — by the time a file
    # reaches this class it is already a local extract.
    class ParquetReader
      THEMES = Downloader::THEMES

      attr_reader :theme

      def initialize(theme: nil)
        @theme = theme
      end

      def each_record(source:, &block)
        raise ArgumentError, "source must be a local file path" unless source.is_a?(String) && File.exist?(source)

        Parquet.each_row(source, &block)
      end

      def record_count(source:)
        raise ArgumentError, "source must be a local file path" unless source.is_a?(String) && File.exist?(source)

        Parquet.metadata(source)["num_rows"]
      end
    end
  end
end
