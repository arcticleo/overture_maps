# frozen_string_literal: true

require "tempfile"
require "open3"
require "json"

module OvertureMaps
  module Import
    class LocationBasedRunner
      COORDS_REGEX = /^-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?$/
      DEFAULT_OUTPUT_DIR = "tmp/overture"

      attr_reader :theme, :location, :model_class, :options

      def initialize(theme:, location:, model_class:, **options)
        @theme = theme
        @location = location
        @model_class = model_class
        @options = options
      end

      def run
        # Determine if location is coordinates or a name
        if coordinates?(location)
          coords = parse_coordinates(location)
          import_from_bbox(coords[:min_lat], coords[:min_lng], coords[:max_lat], coords[:max_lng])
        else
          import_from_division_name(location)
        end
      end

      private

      def coordinates?(str)
        str.to_s.match?(/^-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?[,\s]_-?\d+(\.\d+)?$/)
      end

      def parse_coordinates(str)
        coords = str.to_s.split(/[,\s]+/).map(&:to_f)
        {
          min_lat: [coords[0], coords[2]].min,
          max_lat: [coords[0], coords[2]].max,
          min_lng: [coords[1], coords[3]].min,
          max_lng: [coords[1], coords[3]].max
        }
      end

      def import_from_division_name(name)
        # Search for divisions
        puts "Searching for: #{name}"
        puts

        results = Downloader.search_divisions(query: name)

        if results.empty?
          raise Error, "No divisions found matching '#{name}'"
        end

        selected = select_division(results)
        bbox = selected[:bbox]

        unless bbox
          raise Error, "Could not get bounding box for '#{selected[:name]}'"
        end

        puts "Selected: #{selected[:name]}"
        puts "Bounding box: #{bbox['ymin']}, #{bbox['xmin']} to #{bbox['ymax']}, #{bbox['xmax']}"
        puts

        import_from_bbox(
          bbox["ymin"],
          bbox["xmin"],
          bbox["ymax"],
          bbox["xmax"],
          display_name: selected[:name]
        )
      end

      def select_division(results)
        if results.count == 1
          result = results.first
          location_info = [result[:country], result[:region]].compact.join(" / ")
          puts "Found: #{result[:name]} (#{result[:subtype]})"
          puts "  Location: #{location_info}" unless location_info.empty?
          result
        else
          puts "Multiple matches found:"
          results.each_with_index do |r, i|
            location_info = [r[:country], r[:region]].compact.join(" / ")
            area_info = r[:area_km2] && r[:area_km2] > 0 ? " (#{r[:area_km2]} km²)" : ""
            puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]}) - #{location_info}#{area_info}"
          end
          puts
          print "Enter number to select (or 'q' to quit): "
          input = $stdin.gets&.strip

          if input == 'q' || input.nil?
            puts "Cancelled."
            exit 0
          end

          idx = input.to_i - 1
          unless idx >= 0 && idx < results.count
            puts "Invalid selection."
            exit 1
          end

          results[idx]
        end
      end

      def import_from_bbox(min_lat, min_lng, max_lat, max_lng, display_name: nil)
        # Check for local file first
        local_file = find_local_file(display_name)

        if local_file
          handle_local_file_import(local_file)
        else
          import_from_s3_with_bbox(min_lat, min_lng, max_lat, max_lng)
        end
      end

      def find_local_file(display_name)
        # Look for files matching the theme in the default output directory
        return nil unless Dir.exist?(DEFAULT_OUTPUT_DIR)

        pattern = File.join(DEFAULT_OUTPUT_DIR, "#{theme}_*.parquet")
        files = Dir.glob(pattern)

        # If we have a display name, try to find a file matching it
        if display_name
          # Try exact match first
          exact_match = files.find { |f| f.include?(display_name.downcase.gsub(/\s+/, '_')) }
          return exact_match if exact_match
        end

        # Return the most recently modified file if any exist
        files.sort_by { |f| File.mtime(f) }.last
      end

      def handle_local_file_import(local_file)
        size_mb = (File.size(local_file) / (1024.0 * 1024)).round(2)

        puts "Found local file: #{local_file} (#{size_mb} MB)"
        puts
        puts "Import from this file? (y/n/d)"
        puts "  y - Import from local file (faster)"
        puts "  n - Cancel"
        puts "  d - Download fresh data from S3 (may be newer)"
        puts
        print "Enter choice (y): "

        choice = $stdin.gets&.strip&.downcase
        choice = 'y' if choice.nil? || choice.empty?

        case choice
        when 'y', 'yes'
          puts "Importing from local file..."
          import_from_local_file(local_file)
        when 'd', 'download'
          puts "Downloading fresh data from S3..."
          # Delete the old file and re-download
          File.delete(local_file)
          puts "Removed old file: #{local_file}"
          # Re-run import which will download fresh data
          run
        when 'n', 'no', 'q', 'quit'
          puts "Cancelled."
          exit 0
        else
          puts "Invalid choice. Cancelling."
          exit 1
        end
      end

      def import_from_local_file(local_file)
        runner = Runner.new(
          model_class: model_class,
          batch_size: options[:batch_size] || 1000
        )

        reader = ParquetReader.new(theme: theme)
        filter = build_category_filter(options[:categories])

        runner.import_from_reader(reader, source: local_file, filter: filter)
        print_results(runner)
      end

      def import_from_s3_with_bbox(min_lat, min_lng, max_lat, max_lng)
        puts "Importing from S3 with spatial filtering..."
        puts "  Bounding box: #{min_lat}, #{min_lng} to #{max_lat}, #{max_lng}"
        puts

        Downloader.ensure_duckdb_cli!

        types_to_query = options[:type] ? [options[:type]] : Downloader::TYPES[theme]

        if types_to_query.nil? || types_to_query.empty?
          raise Error, "No types found for theme: #{theme}"
        end

        total_imported = 0
        total_errors = 0

        types_to_query.each do |type|
          puts "Querying #{theme}/#{type} from S3..."

          records = query_s3_for_records(theme, type, min_lat, max_lat, min_lng, max_lng)

          if records.empty?
            puts "  No data found"
            next
          end

          puts "  Found #{records.length} records, importing..."

          runner = Runner.new(
            model_class: model_class,
            batch_size: options[:batch_size] || 1000
          )

          filter = build_category_filter(options[:categories])

          # Convert DuckDB results to record format expected by Runner
          record_enumerator = records.each
          runner.import_from_records(record_enumerator, filter: filter)

          total_imported += runner.imported_count
          total_errors += runner.error_count

          puts "  Imported: #{runner.imported_count}, Errors: #{runner.error_count}"
        end

        puts
        puts "Import Complete!"
        puts "  Total imported: #{total_imported}"
        puts "  Total errors: #{total_errors}"
      end

      def query_s3_for_records(theme, type, min_lat, max_lat, min_lng, max_lng)
        columns = case theme
        when "places"
          "*"
        when "buildings"
          "id, names, height, level, class, is_underground, geometry"
        when "addresses"
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

        results = Downloader.run_duckdb_sql(sql)

        # Convert DuckDB JSON output to record format
        results.map do |row|
          # Parse geometry if it's a JSON object
          if row["geometry"].is_a?(Hash)
            row["geometry"] = row["geometry"].to_json
          end
          row
        end
      end

      def build_category_filter(categories)
        return nil unless categories && categories.any?

        lambda do |record|
          record_categories = record["categories"]
          return false unless record_categories

          cat_hash = case record_categories
          when String
            begin
              JSON.parse(record_categories)
            rescue JSON::ParserError
              return false
            end
          when Hash
            record_categories
          else
            return false
          end

          return false unless cat_hash.is_a?(Hash)

          categories.any? do |wanted|
            cat_hash.key?(wanted) ||
              cat_hash.values.any? { |v| Array(v).include?(wanted) }
          end
        end
      end

      def print_results(runner)
        puts "\nImport Complete!"
        puts "  Imported: #{runner.imported_count}"
        puts "  Errors:   #{runner.error_count}"

        if runner.errors.any? && ENV["VERBOSE"]
          puts "\nErrors:"
          runner.errors.first(10).each do |error|
            puts "  - #{error[:error]}"
          end
        end
      end
    end
  end
end
