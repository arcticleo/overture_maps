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

        # If no location specified, return the most recent file
        return files.sort_by { |f| File.mtime(f) }.last if location.nil?

        # Try to find a file matching the location name
        normalized_location = location.to_s.downcase.gsub(/\s+/, "_")

        # Look for files containing the location name
        files.find { |f| f.downcase.include?(normalized_location) }
      end

      # Query S3 directly using DuckDB with spatial filtering
      # Returns records as an array of hashes
      def self.query_s3_with_bbox(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng:, version: nil, limit: nil, offset: nil)
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

        # Use FIRST to get distinct IDs (more memory efficient than DISTINCT ON)
        sql = <<~SQL.squish
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          SET memory_limit = '4GB';
          SET threads = 4;
          SELECT #{columns}
          FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=#{theme}/type=#{type}/*', union_by_name=true, hive_partitioning=1)
          WHERE bbox.xmin >= #{min_lng}
            AND bbox.xmax <= #{max_lng}
            AND bbox.ymin >= #{min_lat}
            AND bbox.ymax <= #{max_lat}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) = 1
          ORDER BY id
          #{"LIMIT #{limit}" if limit}
          #{"OFFSET #{offset}" if offset && offset > 0}
        SQL

        Downloader.run_duckdb_sql(sql)
      end

      # Get total count of records matching bbox (without loading them all)
      def self.count_s3_with_bbox(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng:, version: nil)
        require "tempfile"
        require "open3"
        require "json"

        Downloader.ensure_duckdb_cli!

        sql = <<~SQL.squish
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          SELECT COUNT(DISTINCT id) as count
          FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=#{theme}/type=#{type}/*', union_by_name=true)
          WHERE bbox.xmin >= #{min_lng}
            AND bbox.xmax <= #{max_lng}
            AND bbox.ymin >= #{min_lat}
            AND bbox.ymax <= #{max_lat}
        SQL

        result = Downloader.run_duckdb_sql(sql)
        result.first&.[]("count") || 0
      end

      # Query all records with batching for large areas - yields records to avoid memory buildup
      def self.query_s3_with_bbox_batched(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng:, version: nil, batch_size: 50000)
        offset = 0

        loop do
          batch = query_s3_with_bbox(
            theme: theme,
            type: type,
            min_lat: min_lat,
            max_lat: max_lat,
            min_lng: min_lng,
            max_lng: max_lng,
            version: version,
            limit: batch_size,
            offset: offset
          )

          break if batch.empty?

          yield batch if block_given?

          offset += batch_size
        end

        nil
      end

      # Stream records from S3 with spatial filtering using DuckDB
      # Yields each record for memory-efficient processing - writes to temp file and streams
      def self.stream_s3_with_bbox(theme:, type:, min_lat:, max_lat:, min_lng:, max_lng:, version: nil, batch_size: 1000, &block)
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
          SET memory_limit = '4GB';
          SET threads = 4;
          COPY (
            SELECT #{columns}
            FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=#{theme}/type=#{type}/*', union_by_name=true, hive_partitioning=1)
            WHERE bbox.xmin >= #{min_lng}
              AND bbox.xmax <= #{max_lng}
              AND bbox.ymin >= #{min_lat}
              AND bbox.ymax <= #{max_lat}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) = 1
            ORDER BY id
          ) TO '/dev/stdout' WITH (FORMAT JSON, ARRAY true)
        SQL

        # Write SQL to temp file
        sql_file = Tempfile.new(["overture_stream", ".sql"])
        sql_file.write(sql)
        sql_file.close

        # Run DuckDB and stream output
        cmd = "#{Downloader.duckdb_cli_path} < #{sql_file.path} 2>&1"

        batch = []
        Downloader.ensure_duckdb_cli!

        IO.popen(cmd, "r") do |io|
          # DuckDB outputs a JSON array, we need to parse it line by line or as a stream
          # For large results, we'll use a streaming JSON parser
          content = io.read

          # Clean up temp file
          sql_file.unlink

          # Parse the JSON array
          begin
            records = JSON.parse(content)
            records.each do |record|
              batch << record
              if batch.length >= batch_size
                yield_batch(batch, &block)
                batch = []
              end
            end
          rescue JSON::ParserError => e
            # If JSON is malformed, it might be a huge single array - try streaming parser
            puts "Warning: Large JSON result, using streaming parser..."
            yield_from_streaming_json(content, batch_size, &block)
            return
          end
        end

        # Yield remaining records
        yield_batch(batch, &block) if batch.any?
      end

      def self.yield_batch(batch)
        return if batch.empty?
        yield batch
      end

      def self.yield_from_streaming_json(content, batch_size)
        # For very large results, parse JSON incrementally
        # This handles the case where JSON is too large to parse at once
        records = []
        depth = 0
        in_string = false
        escape = false
        buffer = String.new

        content.each_char do |char|
          if escape
            buffer << char
            escape = false
            next
          end

          if char == '\\'
            buffer << char
            escape = true
            next
          end

          if char == '"' && !in_string
            in_string = true
            buffer << char
            next
          end

          if char == '"' && in_string
            in_string = false
            buffer << char
            next
          end

          if in_string
            buffer << char
            next
          end

          case char
          when '['
            depth += 1
            if depth == 2
              # Start of an object inside the array
              buffer = String.new
            end
          when ']'
            depth -= 1
            if depth == 1 && !buffer.empty?
              # End of an object
              begin
                records << JSON.parse(buffer)
                if records.length >= batch_size
                  yield records
                  records = []
                end
              rescue JSON::ParserError
                # Skip malformed objects
              end
              buffer = String.new
            end
          when ','
            if depth == 2 && !buffer.empty?
              # End of an object
              begin
                records << JSON.parse(buffer)
                if records.length >= batch_size
                  yield records
                  records = []
                end
              rescue JSON::ParserError
                # Skip malformed objects
              end
              buffer = String.new
            elsif depth > 1
              buffer << char
            end
          else
            buffer << char if depth > 1
          end
        end

        yield records if records.any?
      end
      private_class_method :yield_batch, :yield_from_streaming_json
    end
  end
end
