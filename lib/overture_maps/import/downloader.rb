# frozen_string_literal: true

module OvertureMaps
  module Import
    class Downloader
      THEMES = %w[addresses base buildings divisions places transportation].freeze

      # Feature types within each theme
      TYPES = {
        "addresses" => %w[address],
        "base" => %w[bathymetry infrastructure land land_cover land_use water],
        "buildings" => %w[building building_part],
        "divisions" => %w[division division_area division_boundary],
        "places" => %w[place],
        "transportation" => %w[connector segment]
      }.freeze

      DUCKDB_VERSION = "1.1.0"
      S3_BUCKET = "overturemaps-us-west-2"
      S3_REGION = "us-west-2"
      AZURE_ACCOUNT = "overturemapswestus2"
      AZURE_CONTAINER = "release"

      attr_reader :theme, :type, :version, :output_dir

      def initialize(theme:, type: nil, version: nil, output_dir: nil)
        @theme = theme
        @type = type
        @version = version || self.class.latest_version
        @output_dir = output_dir || Dir.pwd
      end

      # Get types for a theme
      def self.types_for_theme(theme)
        TYPES[theme] || []
      end

      # Get all themes with their types
      def self.themes_with_types
        TYPES
      end

      # Download files from S3
      def download_from_s3
        require "aws-sdk-s3"

        # For public buckets, use unsigned requests by setting stub credentials
        # and disabling signature verification
        Aws.config.update(
          region: S3_REGION,
          credentials: Aws::Credentials.new("x", "x")
        )
        s3 = Aws::S3::Client.new
        bucket = S3_BUCKET

        objects = list_s3_objects(s3, bucket)

        if objects.empty?
          puts "No files found for #{theme}#{type ? "/#{type}" : ""}"
          return 0
        end

        puts "Found #{objects.count} file(s) to download..."
        puts

        objects.each do |obj|
          key = obj.key
          filename = File.basename(key)
          local_path = File.join(output_dir, filename)

          # Skip if file exists (allow resume)
          if File.exist?(local_path) && File.size(local_path) == obj.size
            puts "Skipping #{filename} (already exists)"
            next
          end

          puts "Downloading #{filename} (#{format_size(obj.size)})..."
          s3.get_object(
            bucket: bucket,
            key: key,
            response_target: local_path
          )
          puts "  Saved to #{local_path}"
        end

        puts
        puts "Download complete!"
        objects.count
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      # Download files from Azure Blob Storage
      def download_from_azure
        require "azure/storage/blob"

        access_key = ENV["AZURE_STORAGE_ACCESS_KEY"]
        unless access_key
          raise Error, "AZURE_STORAGE_ACCESS_KEY environment variable not set"
        end

        blob_service = Azure::Storage::Blob::BlobService.create(
          storage_account_name: AZURE_ACCOUNT,
          storage_access_key: access_key
        )

        container = AZURE_CONTAINER
        prefix = build_blob_prefix

        blobs = list_blobs(blob_service, container, prefix)

        if blobs.empty?
          puts "No files found for #{theme}#{type ? "/#{type}" : ""}"
          return 0
        end

        puts "Found #{blobs.count} file(s) to download..."
        puts

        blobs.each do |blob|
          filename = blob.name.split("/").last
          local_path = File.join(output_dir, filename)

          puts "Downloading #{filename}..."
          blob_service.get_blob_to_path(container, blob.name, local_path)
          puts "  Saved to #{local_path}"
        end

        puts
        puts "Download complete!"
        blobs.count
      rescue LoadError
        raise Error, "Azure SDK not installed. Run: gem install azure-storage-blob"
      rescue Azure::Storage::Error => e
        raise Error, "Azure download failed: #{e.message}"
      end

      # Download data for a geographic bounding box using DuckDB
      # This is more efficient than downloading all files as it filters server-side
      def download_from_s3_with_bbox(lat1:, lng1:, lat2:, lng2:, format: "parquet", display_name: nil)
        self.class.ensure_duckdb_cli!

        # Normalize coordinates (handle cases where user passes corners in any order)
        min_lat = [lat1, lat2].min
        max_lat = [lat1, lat2].max
        min_lng = [lng1, lng2].min
        max_lng = [lng1, lng2].max

        # Build the query
        types_to_query = type ? [type] : TYPES[theme] || []

        output_files = []
        types_to_query.each do |t|
          puts "Querying #{theme}/#{t}..."

          query = build_bbox_query(t, min_lat, max_lat, min_lng, max_lng)

          # Use display_name in filename if provided, otherwise use coordinates
          filename_suffix = display_name || "#{min_lat}_#{max_lat}_#{min_lng}_#{max_lng}"
          filename = "#{theme}_#{t}_#{filename_suffix}.#{format_extension(format)}"
          local_path = File.join(output_dir, filename)

          puts "  Exporting to #{filename}..."
          run_duckdb_query(query, local_path, format)

          if File.exist?(local_path) && File.size(local_path) > 0
            output_files << local_path
            puts "  Saved #{format_size(File.size(local_path))}"
          else
            puts "  No data found"
          end
        end

        puts
        puts "Download complete!"
        output_files.count
      end

      # Download data for a center point and radius using DuckDB
      def download_from_s3_nearby(center_lat:, center_lng:, radius_meters:, format: "parquet")
        # Convert radius to approximate lat/lng delta
        # 1 degree latitude ≈ 111,000 meters
        # 1 degree longitude ≈ 111,000 * cos(latitude) meters

        lat_delta = radius_meters.to_f / 111_000
        lng_delta = radius_meters.to_f / (111_000 * Math.cos(Math.radians(center_lat)))

        min_lat = center_lat - lat_delta
        max_lat = center_lat + lat_delta
        min_lng = center_lng - lng_delta
        max_lng = center_lng + lng_delta

        download_from_s3_with_bbox(
          lat1: min_lat,
          lng1: min_lng,
          lat2: max_lat,
          lng2: max_lng,
          format: format
        )
      end

      # List available files (without downloading)
      def list_files(provider: :s3)
        case provider
        when :s3
          list_files_from_s3
        when :azure
          list_files_from_azure
        else
          raise ArgumentError, "Unknown provider: #{provider}"
        end
      end

      # Get the S3 URI pattern for this theme/type
      def s3_uri_pattern
        base = "s3://#{S3_BUCKET}/release/#{version}"
        "#{base}/theme=#{theme}#{type ? "/type=#{type}" : ""}/*.parquet"
      end

      # Get the Azure URI pattern for this theme/type
      def azure_uri_pattern
        base = "https://#{AZURE_ACCOUNT}.blob.core.windows.net/#{AZURE_CONTAINER}/release/#{version}"
        "#{base}/theme=#{theme}#{type ? "/type=#{type}" : ""}/*.parquet"
      end

      # List available types for a theme
      def self.list_types(theme:, version: nil)
        v = version || latest_version

        require "aws-sdk-s3" unless defined?(Aws::S3::Client)

        s3 = Aws::S3::Client.new(region: S3_REGION)
        prefix = "release/#{v}/theme=#{theme}/type="

        objects = s3.list_objects_v2(bucket: S3_BUCKET, prefix: prefix)
        objects.contents.map { |o| o.key.split("/")[2]&.gsub("type=", "") }.compact.uniq.sort
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      # List available themes
      def self.list_themes
        require "aws-sdk-s3" unless defined?(Aws::S3::Client)

        s3 = Aws::S3::Client.new(region: S3_REGION)
        prefix = "release/"

        objects = s3.list_objects_v2(bucket: S3_BUCKET, prefix: prefix, delimiter: "/")
        objects.common_prefixes.map { |o| o.prefix.split("/")[-2]&.gsub("theme=", "") }.compact.uniq.sort
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      STAC_CATALOG_URL = "https://stac.overturemaps.org/catalog.json"

      # Fetch the STAC catalog (cached for the process lifetime)
      def self.stac_catalog
        @stac_catalog ||= begin
          require "net/http"
          require "json"

          uri = URI(STAC_CATALOG_URL)
          JSON.parse(Net::HTTP.get(uri))
        rescue StandardError => e
          warn "Warning: Could not fetch STAC catalog: #{e.message}"
          {}
        end
      end

      # List available versions from the STAC catalog
      def self.list_versions
        links = stac_catalog["links"] || []
        links
          .select { |l| l["rel"] == "child" }
          .filter_map { |l| l["href"]&.match(%r{(\d{4}-\d{2}-\d{2}\.\d+)/})&.[](1) }
          .sort
          .reverse
      end

      # Get the latest version from the Overture STAC catalog
      def self.latest_version
        @latest_version ||= stac_catalog["latest"]
      end

      def self.reset_latest_version!
        @stac_catalog = nil
        @latest_version = nil
      end

      # Search for divisions by name
      def self.search_divisions(query:, version: nil)
        ensure_duckdb_cli!

        # Use recursive glob to search across all versions (divisions data may be in different versions)
        # Note: We deduplicate in Ruby since DISTINCT ON requires ordering by id first
        sql = <<~SQL
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          SELECT id, names.primary as name, subtype, country, region, population, bbox.xmin, bbox.xmax, bbox.ymin, bbox.ymax,
                 (bbox.xmax - bbox.xmin) * (bbox.ymax - bbox.ymin) as bbox_area
          FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=divisions/*/*.parquet', union_by_name=true)
          WHERE names.primary ILIKE '%#{query}%'
            AND subtype IN ('country', 'region', 'subregion', 'locality', 'macrohood', 'neighborhood')
            AND bbox.xmax > bbox.xmin
            AND bbox.ymax > bbox.ymin
          ORDER BY
            CASE WHEN LOWER(names.primary) = LOWER('#{query}') THEN 0 ELSE 1 END,
            bbox_area DESC
          LIMIT 100
        SQL

        results = run_duckdb_sql(sql)
        # Deduplicate by ID - divisions may appear in multiple source files
        results = results.uniq { |row| row["id"] }
        # Limit to 50 after deduplication to show exact matches + partial matches
        results = results.first(50)
        mapped = results.map do |row|
          ymin = row["ymin"].to_f
          ymax = row["ymax"].to_f
          xmin = row["xmin"].to_f
          xmax = row["xmax"].to_f

          lat_center = (ymin + ymax) / 2.0
          km_per_deg_lat = 111.0
          km_per_deg_lng = 111.0 * Math.cos(lat_center * Math::PI / 180.0)

          width_km = (xmax - xmin) * km_per_deg_lng
          height_km = (ymax - ymin) * km_per_deg_lat
          area_km2 = (width_km * height_km).round(2)

          {
            id: row["id"],
            name: row["name"] || query,
            subtype: row["subtype"],
            country: row["country"],
            region: row["region"],
            population: row["population"],
            bbox: {
              "xmin" => row["xmin"],
              "xmax" => row["xmax"],
              "ymin" => row["ymin"],
              "ymax" => row["ymax"]
            },
            bbox_area: row["bbox_area"],
            area_km2: area_km2
          }
        end
      end

      # Get bbox from a division ID
      def self.get_division_bbox(division_id:, version: nil)
        ensure_duckdb_cli!

        sql = <<~SQL
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          SELECT bbox FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=divisions/*/*.parquet', union_by_name=true)
          WHERE id = '#{division_id}'
          LIMIT 1
        SQL

        result = run_duckdb_sql(sql).first
        result&.fetch("bbox")
      end

      # Ensure DuckDB CLI is available, download if needed
      def self.ensure_duckdb_cli!
        return if duckdb_cli_path

        puts "Downloading DuckDB CLI..."
        arch = RUBY_PLATFORM =~ /darwin/ ? "duckdb_cli-osx-universal" : "duckdb_cli-linux-amd64"
        url = "https://github.com/duckdb/duckdb/releases/download/v#{DUCKDB_VERSION}/#{arch}.zip"

        zip_path = "/tmp/duckdb.zip"
        File.binwrite(zip_path, Net::HTTP.get(URI(url)))
        system("unzip -o -j #{zip_path} -d /tmp/duckdb/ 2>/dev/null && chmod +x /tmp/duckdb/duckdb")
        File.delete(zip_path) if File.exist?(zip_path)

        raise "Failed to download DuckDB CLI" unless duckdb_cli_path && File.executable?(duckdb_cli_path)
      end

      def self.duckdb_cli_path
        return @duckdb_path if @duckdb_path && File.executable?(@duckdb_path)

        # Check common locations
        paths = ["/tmp/duckdb/duckdb", "duckdb"]
        paths += `which duckdb 2>/dev/null`.split if `which duckdb 2>/dev/null`.present?

        @duckdb_path = paths.find { |p| File.executable?(p) }
      end

      # Run a DuckDB query that exports to a file
      def run_duckdb_query(query, output_path, format)
        require "tempfile"
        require "open3"

        # Use native Parquet export instead of GDAL for better compatibility
        copy_sql = if format.to_s.downcase == "parquet"
          "COPY (#{query}) TO '#{output_path}' (FORMAT PARQUET);"
        else
          driver = gdal_driver(format)
          "COPY (#{query}) TO '#{output_path}' WITH (FORMAT GDAL, DRIVER '#{driver}');"
        end

        sql = <<~SQL
          INSTALL spatial;
          LOAD spatial;
          SET s3_region='us-west-2';
          #{copy_sql}
        SQL

        # Write SQL to temp file
        sql_file = Tempfile.new(["overture_query", ".sql"])
        sql_file.write(sql)
        sql_file.close

        # Run query from file
        cmd = "#{self.class.duckdb_cli_path} < #{sql_file.path} 2>&1"
        output, status = Open3.capture2(cmd)

        sql_file.unlink

        raise "DuckDB error: #{output}" unless status.success?
      end

      def self.run_duckdb_sql(sql, output_path: nil)
        require "json"
        require "tempfile"
        require "open3"

        # Write SQL to a temp file
        sql_file = Tempfile.new(["overture_query", ".sql"])
        sql_file.write(sql)
        sql_file.close

        # Run query from file using Open3 for proper output capture
        cmd = "#{duckdb_cli_path} -json < #{sql_file.path} 2>&1"
        output, status = Open3.capture2(cmd)

        sql_file.unlink

        raise "DuckDB error: #{output.empty? ? 'command failed with no output' : output}" unless status.success?

        # Parse JSON output - DuckDB outputs a JSON array
        output = output.strip
        return [] if output.empty?

        JSON.parse(output)
      end

      # Download data for a division (uses DuckDB to get bbox from division shape)
      def download_for_division(division_name:, format: "parquet")
        # Search for the division
        results = self.class.search_divisions(query: division_name, version: version)

        if results.empty?
          raise Error, "No divisions found matching '#{division_name}'"
        end

        if results.count == 1
          selected = results.first
          location_info = [selected[:country], selected[:region]].compact.join(" / ")
          puts "Found: #{selected[:name]} (#{selected[:subtype]})"
          puts "  Location: #{location_info}" unless location_info.empty?
        else
          puts "Multiple matches found for '#{division_name}':"
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

          selected = results[idx]
          location_info = [selected[:country], selected[:region]].compact.join(" / ")
          puts "Selected: #{selected[:name]} (#{location_info})"
        end

        bbox = selected[:bbox]
        unless bbox
          # Try to get bbox from the division_id
          bbox = self.class.get_division_bbox(division_id: selected[:id], version: version)
        end

        unless bbox
          raise Error, "Could not get bounding box for '#{selected[:name]}'"
        end

        # Extract bbox values
        min_lat = bbox["ymin"]
        max_lat = bbox["ymax"]
        min_lng = bbox["xmin"]
        max_lng = bbox["xmax"]

        puts "Bounding box: #{min_lat}, #{min_lng} to #{max_lat}, #{max_lng}"
        puts

        # Download data for this bbox
        download_from_s3_with_bbox(
          lat1: min_lat,
          lng1: min_lng,
          lat2: max_lat,
          lng2: max_lng,
          format: format
        )
      end

      private

      def build_s3_prefix
        "release/#{version}/theme=#{theme}#{type ? "/type=#{type}" : ""}"
      end

      def build_blob_prefix
        "release/#{version}/theme=#{theme}#{type ? "/type=#{type}" : ""}"
      end

      def build_bbox_query(type, min_lat, max_lat, min_lng, max_lng)
        # Select common columns plus geometry
        columns = case theme
        when "places"
          "*"
        when "buildings"
          "id, names, height, level, class, is_underground, geometry"
        when "addresses"
          "*"
        else
          # For complex themes (base, divisions, transportation), select all columns
          # These themes have varying schemas across different types
          "*"
        end

        # Use DISTINCT ON (id) to deduplicate records with same ID
        # This prevents duplicates from multiple source files
        <<~SQL.squish
          SELECT DISTINCT ON (id) #{columns}
          FROM read_parquet('s3://overturemaps-us-west-2/release/**/theme=#{theme}/type=#{type}/*', union_by_name=true)
          WHERE bbox.xmin > #{min_lng}
            AND bbox.xmax < #{max_lng}
            AND bbox.ymin > #{min_lat}
            AND bbox.ymax < #{max_lat}
          ORDER BY id
        SQL
      end

      def format_extension(format)
        case format.downcase
        when "geojson" then "geojson"
        when "gpkg", "geopackage" then "gpkg"
        when "shp", "shapefile" then "shp"
        else "parquet"
        end
      end

      def gdal_driver(format)
        case format.downcase
        when "geojson" then "GeoJSON"
        when "gpkg", "geopackage" then "GPKG"
        when "shp", "shapefile" then "ESRI Shapefile"
        else "Parquet"
        end
      end

      def list_s3_objects(s3, bucket)
        prefix = build_s3_prefix
        objects = s3.list_objects_v2(bucket: bucket, prefix: prefix)
        objects.contents.select { |o| o.key.end_with?(".parquet") }
      end

      def list_files_from_s3
        require "aws-sdk-s3" unless defined?(Aws::S3::Client)

        s3 = Aws::S3::Client.new(region: S3_REGION)
        objects = list_s3_objects(s3, S3_BUCKET)

        objects.map do |obj|
          {
            key: obj.key,
            size: obj.size,
            last_modified: obj.last_modified
          }
        end
      rescue LoadError
        raise Error, "AWS SDK not installed. Run: gem install aws-sdk-s3"
      end

      def list_files_from_azure
        require "azure/storage/blob" unless defined?(Azure::Storage::Blob)

        access_key = ENV["AZURE_STORAGE_ACCESS_KEY"]
        unless access_key
          raise Error, "AZURE_STORAGE_ACCESS_KEY environment variable not set"
        end

        blob_service = Azure::Storage::Blob::BlobService.create(
          storage_account_name: AZURE_ACCOUNT,
          storage_access_key: access_key
        )

        blobs = list_blobs(blob_service, AZURE_CONTAINER, build_blob_prefix)

        blobs.map do |blob|
          {
            key: blob.name,
            size: blob.properties[:content_length],
            last_modified: blob.properties[:last_modified]
          }
        end
      rescue LoadError
        raise Error, "Azure SDK not installed. Run: gem install azure-storage-blob"
      rescue Azure::Storage::Error => e
        raise Error, "Azure listing failed: #{e.message}"
      end

      def list_blobs(blob_service, container, prefix)
        blobs = []
        marker = nil

        loop do
          result = blob_service.list_blobs(container, marker: marker, prefix: prefix)
          blobs.concat(result.blobs)
          break unless result.continuation_token
          marker = result.continuation_token
        end

        blobs.select { |b| b.name.end_with?(".parquet") }
      end

      def format_size(bytes)
        if bytes >= 1_073_741_824
          "#{(bytes / 1_073_741_824.0).round(2)} GB"
        elsif bytes >= 1_048_576
          "#{bytes / 1_048_576.0.round(2)} MB"
        elsif bytes >= 1024
          "#{bytes / 1024.0.round(2)} KB"
        else
          "#{bytes} bytes"
        end
      end
    end
  end
end
