# frozen_string_literal: true

namespace :overture_maps do
  namespace :download do
    desc "Download places Parquet files from S3, or by location name (e.g., rails overture_maps:download:places[Seattle])"
    task :places, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "places"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "place",
        version: version,
        output_dir: output_dir
      )

      # Check if location looks like coordinates with optional display name (e.g., "47.6,-122.3,47.7,-122.2|seattle")
      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        # Split coords from display name
        coord_part, display_name = location.split("|", 2)
        # Format: lat1,lng1,lat2,lng2 (4 coordinates, comma or underscore separated)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading places within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        # Treat as location name
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        # Download all places
        puts "Downloading all places#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download buildings Parquet files from S3, or by location name"
    task :buildings, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "buildings"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "building",
        version: version,
        output_dir: output_dir
      )

      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        coord_part, display_name = location.split("|", 2)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading buildings within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        puts "Downloading all buildings#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download addresses Parquet files from S3, or by location name"
    task :addresses, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "addresses"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "address",
        version: version,
        output_dir: output_dir
      )

      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        coord_part, display_name = location.split("|", 2)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading addresses within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        puts "Downloading all addresses#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download divisions Parquet files from S3, or by location name"
    task :divisions, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "divisions"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "division",
        version: version,
        output_dir: output_dir
      )

      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        coord_part, display_name = location.split("|", 2)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading divisions within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        puts "Downloading all divisions#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download transportation Parquet files from S3, or by location name"
    task :transportation, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "transportation"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "segment",
        version: version,
        output_dir: output_dir
      )

      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        coord_part, display_name = location.split("|", 2)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading transportation within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        puts "Downloading all transportation#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download base Parquet files from S3, or by location name (land, water, etc.)"
    task :base, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "base"
      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      FileUtils.mkdir_p(output_dir)

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: "land",
        version: version,
        output_dir: output_dir
      )

      if location && location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
        coord_part, display_name = location.split("|", 2)
        coords = coord_part.split(/[_,]/).map(&:to_f)
        puts "Downloading base data within bounding box: #{coords.join(', ')}..."
        puts "  Display name: #{display_name}" if display_name
        count = downloader.download_from_s3_with_bbox(
          lat1: coords[0],
          lng1: coords[1],
          lat2: coords[2],
          lng2: coords[3],
          display_name: display_name
        )
        puts "\nDownloaded data for #{count} type(s)"
      elsif location
        puts "Searching for location: #{location}..."
        begin
          count = downloader.download_for_division(division_name: location)
          puts "\nDownloaded data for #{count} type(s)"
        rescue OvertureMaps::Import::Error => e
          puts "Error: #{e.message}"
          exit 1
        end
      else
        puts "Downloading all base data#{" (version: #{version})" if version} from S3..."
        count = downloader.download_from_s3
        puts "\nDownloaded #{count} file(s)"
      end
    end

    desc "Download all themes from S3 (global data only), or by location name (e.g., rails overture_maps:download:all[Seattle])"
    task :all, [:location, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      location = args[:location]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      themes = %w[places buildings addresses divisions transportation base]

      # If location is provided, resolve it once and get bbox
      bbox_coords = nil
      if location
        # Check if it's already a bounding box (with or without display name)
        if location.match?(/^-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*[_,]-?\d+\.?\d*(\|.+)?$/)
          bbox_coords = location
        else
          # Search for the division once
          puts "Searching for location: #{location}..."
          puts

          results = OvertureMaps::Import::Downloader.search_divisions(query: location, version: version)

          if results.empty?
            puts "Error: No divisions found matching '#{location}'"
            exit 1
          end

          selected = if results.count == 1
            results.first
          else
            puts "Multiple matches found for '#{location}':"
            results.each_with_index do |r, i|
              location_parts = [r[:country], r[:region]].compact
              location_str = location_parts.any? ? " - #{location_parts.join(" / ")}" : ""
              area_info = r[:area_km2] && r[:area_km2] > 0 ? " (#{r[:area_km2]} km²)" : ""
              puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]})#{location_str}#{area_info}"
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

          bbox = selected[:bbox]
          unless bbox
            puts "Error: Could not get bounding box for '#{selected[:name]}'"
            exit 1
          end

          location_info = [selected[:country], selected[:region]].compact.join(" / ")
          puts "Selected: #{selected[:name]} (#{location_info})"
          puts "Bounding box: #{bbox['ymin']}, #{bbox['xmin']} to #{bbox['ymax']}, #{bbox['xmax']}"
          puts

          # Convert bbox to coordinate string with display name
          display_name = selected[:name].downcase.gsub(/[^a-z0-9]+/, '_').gsub(/^_+|_+$/, '')
          bbox_coords = "#{bbox['ymin']},#{bbox['xmin']},#{bbox['ymax']},#{bbox['xmax']}|#{display_name}"
        end
      end

      themes.each do |theme|
        puts "\n--- Downloading #{theme} ---"

        # Invoke the individual theme task with bbox coordinates instead of name
        # This avoids re-searching for each theme
        theme_task = Rake::Task["overture_maps:download:#{theme}"]
        theme_task.invoke(bbox_coords, version, output_dir)
        theme_task.reenable
      end

      puts "\n=== All downloads complete! ==="
    end

    desc "Download from Azure Blob Storage"
    task :azure, [:theme, :region, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = args[:theme] || "places"
      region = args[:region]
      version = args[:version]
      output_dir = args[:output_dir]

      unless ENV["AZURE_STORAGE_ACCESS_KEY"]
        puts "Error: AZURE_STORAGE_ACCESS_KEY environment variable not set"
        puts "Set it with: export AZURE_STORAGE_ACCESS_KEY='your-key'"
        exit 1
      end

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        region: region,
        version: version,
        output_dir: output_dir
      )

      puts "Downloading #{theme}#{" (#{region})" if region} from Azure..."

      count = downloader.download_from_azure
      puts "\nDownloaded #{count} file(s)"
    end

    desc "List available regions for a theme"
    task :regions, [:theme, :version] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = args[:theme] || "places"
      version = args[:version]

      regions = OvertureMaps::Import::Downloader.list_regions(theme: theme, version: version)

      puts "Available regions for #{theme}:"
      regions.each { |r| puts "  - #{r}" }
    end

    desc "List available versions"
    task :versions do |_t, _args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      versions = OvertureMaps::Import::Downloader.list_versions

      puts "Available versions:"
      versions.each { |v| puts "  - #{v}" }
    end

    desc "List available themes"
    task :themes do |_t, _args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      puts "Available themes:"
      OvertureMaps::Import::Downloader.themes.each { |t| puts "  - #{t}" }
    end

    desc "List files available for download"
    task :list, [:theme, :region, :version, :provider] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = args[:theme] || "places"
      region = args[:region]
      version = args[:version]
      provider = (args[:provider] || "s3").to_sym

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        region: region,
        version: version
      )

      files = downloader.list_files(provider: provider)

      puts "Available files for #{theme}#{" (#{region})" if region}:"
      files.each do |f|
        size_mb = f[:size] ? (f[:size] / 1_000_000.0).round(2) : "unknown"
        puts "  - #{f[:key].split("/").last} (#{size_mb} MB)"
      end
    end
  end

  namespace :import do
    desc "Import places from a Parquet file"
    task :places, [:file_path] do |_t, args|
      require "overture_maps"
      require "overture_maps/import"

      file_path = args[:file_path]
      raise "Please provide a file path: rake overture_maps:import:places[/path/to/places.parquet]" unless file_path

      puts "Importing places from #{file_path}..."

      reader = OvertureMaps::Import::ParquetReader.new(theme: "places")

      runner = OvertureMaps::Import::Runner.new(
        model_class: OverturePlace,
        batch_size: 1000
      )

      transform = ->(record) {
        {
          id: record["id"],
          names: record["names"],
          categories: record["categories"]&.to_json,
          brands: record["brands"]&.to_json,
          addresses: record["addresses"]&.to_json,
          confidence: record["confidence"],
          elevation: record["elevation"],
          country: record["country"],
          geometry: parse_geometry(record["geometry"]),
          created_at: Time.current,
          updated_at: Time.current
        }.compact
      }

      runner.import_from_file(file_path, transform: transform)

      puts "\nImported: #{runner.imported_count}"
      puts "Errors: #{runner.error_count}"
    end

    desc "Import buildings from a Parquet file"
    task :buildings, [:file_path] do |_t, args|
      require "overture_maps"
      require "overture_maps/import"

      file_path = args[:file_path]
      raise "Please provide a file path: rake overture_maps:import:buildings[/path/to/buildings.parquet]" unless file_path

      puts "Importing buildings from #{file_path}..."

      runner = OvertureMaps::Import::Runner.new(
        model_class: OvertureBuilding,
        batch_size: 1000
      )

      transform = ->(record) {
        {
          id: record["id"],
          names: record["names"],
          height: record["height"],
          level: record["level"],
          class: record["class"],
          is_underground: record["is_underground"],
          geometry: parse_geometry(record["geometry"]),
          created_at: Time.current,
          updated_at: Time.current
        }.compact
      }

      runner.import_from_file(file_path, transform: transform)

      puts "\nImported: #{runner.imported_count}"
      puts "Errors: #{runner.error_count}"
    end

    desc "Import addresses from a Parquet file"
    task :addresses, [:file_path] do |_t, args|
      require "overture_maps"
      require "overture_maps/import"

      file_path = args[:file_path]
      raise "Please provide a file path: rake overture_maps:import:addresses[/path/to/addresses.parquet]" unless file_path

      puts "Importing addresses from #{file_path}..."

      runner = OvertureMaps::Import::Runner.new(
        model_class: OvertureAddress,
        batch_size: 1000
      )

      transform = ->(record) {
        {
          id: record["id"],
          street: record["street"],
          locality: record["locality"],
          region: record["region"],
          country: record["country"],
          postcode: record["postcode"],
          geometry: parse_geometry(record["geometry"]),
          created_at: Time.current,
          updated_at: Time.current
        }.compact
      }

      runner.import_from_file(file_path, transform: transform)

      puts "\nImported: #{runner.imported_count}"
      puts "Errors: #{runner.error_count}"
    end

    desc "Show Parquet file record count"
    task :count, [:file_path] do |_t, args|
      require "overture_maps"
      require "overture_maps/import"

      file_path = args[:file_path]
      raise "Please provide a file path" unless file_path

      reader = OvertureMaps::Import::ParquetReader.new(theme: "places")
      count = reader.record_count(source: file_path)

      puts "Records in #{file_path}: #{count}"
    end

    desc "List available themes"
    task :themes do |_t, _args|
      require "overture_maps"
      require "overture_maps/import"

      puts "Available themes:"
      OvertureMaps::Import::ParquetReader::THEMES.each do |theme|
        puts "  - #{theme}"
      end
    end
  end
end

def parse_geometry(geom)
  return nil unless geom

  factory = RGeo::Geographic.spherical_factory(srid: 4326)

  case geom
  when String
    begin
      factory.parse_wkb(geom)
    rescue RGeo::Error
      RGeo::GeoJSON.decode(geom, geo_factory: factory)
    end
  when Hash
    RGeo::GeoJSON.decode(geom, geo_factory: factory)
  else
    nil
  end
end
