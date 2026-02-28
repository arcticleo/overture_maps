# frozen_string_literal: true

namespace :overture_maps do
  namespace :download do
    desc "Download places Parquet files from S3"
    task :places, [:region, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "places"
      region = args[:region]
      version = args[:version]
      output_dir = args[:output_dir]

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        region: region,
        version: version,
        output_dir: output_dir
      )

      puts "Downloading #{theme}#{" (#{region})" if region}#{" (version: #{version})" if version} from S3..."

      count = downloader.download_from_s3
      puts "\nDownloaded #{count} file(s)"
    end

    desc "Download buildings Parquet files from S3"
    task :buildings, [:region, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "buildings"
      region = args[:region]
      version = args[:version]
      output_dir = args[:output_dir]

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        region: region,
        version: version,
        output_dir: output_dir
      )

      puts "Downloading #{theme}#{" (#{region})" if region}#{" (version: #{version})" if version} from S3..."

      count = downloader.download_from_s3
      puts "\nDownloaded #{count} file(s)"
    end

    desc "Download addresses Parquet files from S3"
    task :addresses, [:region, :version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      theme = "addresses"
      region = args[:region]
      version = args[:version]
      output_dir = args[:output_dir]

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        region: region,
        version: version,
        output_dir: output_dir
      )

      puts "Downloading #{theme}#{" (#{region})" if region}#{" (version: #{version})" if version} from S3..."

      count = downloader.download_from_s3
      puts "\nDownloaded #{count} file(s)"
    end

    desc "Download all themes from S3 (global data only)"
    task :all, [:version, :output_dir] do |_t, args|
      require "overture_maps"
      require "overture_maps/import/downloader"

      version = args[:version]
      output_dir = args[:output_dir]

      %w[places buildings addresses].each do |theme|
        downloader = OvertureMaps::Import::Downloader.new(
          theme: theme,
          version: version,
          output_dir: output_dir
        )

        puts "\n--- Downloading #{theme} ---"
        count = downloader.download_from_s3
        puts "Downloaded #{count} file(s)"
      end
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
