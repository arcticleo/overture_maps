# frozen_string_literal: true

require "rake"
require "overture_maps"
require "overture_maps/import/downloader"

namespace :overture_maps do
  namespace :download do
    # Places
    desc "Download places Parquet files from S3"
    task :places, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "places",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Buildings
    desc "Download buildings Parquet files from S3"
    task :buildings, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "buildings",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Addresses
    desc "Download addresses Parquet files from S3"
    task :addresses, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "addresses",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Base
    desc "Download base data Parquet files from S3"
    task :base, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "base",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Divisions
    desc "Download divisions Parquet files from S3"
    task :divisions, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "divisions",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Transportation
    desc "Download transportation Parquet files from S3"
    task :transportation, [:type, :version, :output_dir] do |_t, args|
      download_files(
        theme: "transportation",
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture",
        provider: :s3
      )
    end

    # Download all themes
    desc "Download all themes from S3"
    task :all, [:type, :version, :output_dir] do |_t, args|
      type = args[:type]
      version = args[:version]
      output_dir = args[:output_dir] || "tmp/overture"

      %w[places buildings addresses base divisions transportation].each do |theme|
        Rake::Task["overture_maps:download:#{theme}"].invoke(type, version, output_dir)
        Rake::Task["overture_maps:download:#{theme}"].reenable
      end

      puts "\nAll downloads complete!"
    end

    # Azure namespace
    namespace :azure do
      desc "Download places Parquet files from Azure"
      task :places, [:type, :version, :output_dir] do |_t, args|
        download_files(
          theme: "places",
          type: args[:type],
          version: args[:version],
          output_dir: args[:output_dir] || "tmp/overture",
          provider: :azure
        )
      end

      desc "Download buildings Parquet files from Azure"
      task :buildings, [:type, :version, :output_dir] do |_t, args|
        download_files(
          theme: "buildings",
          type: args[:type],
          version: args[:version],
          output_dir: args[:output_dir] || "tmp/overture",
          provider: :azure
        )
      end

      desc "Download addresses Parquet files from Azure"
      task :addresses, [:type, :version, :output_dir] do |_t, args|
        download_files(
          theme: "addresses",
          type: args[:type],
          version: args[:version],
          output_dir: args[:output_dir] || "tmp/overture",
          provider: :azure
        )
      end
    end

    # Info/listing tasks
    desc "List available versions"
    task :versions do |_t, _args|
      versions = OvertureMaps::Import::Downloader.list_versions

      puts "Available versions:"
      versions.each { |v| puts "  - #{v}" }

      puts
      latest = OvertureMaps::Import::Downloader.latest_version
      puts "Latest: #{latest}"
    end

    desc "List available themes"
    task :themes do |_t, _args|
      themes = OvertureMaps::Import::Downloader.list_themes

      puts "Available themes:"
      themes.each do |theme|
        types = OvertureMaps::Import::Downloader.types_for_theme(theme)
        puts "  - #{theme}"
        types.each { |t| puts "      #{t}" }
      end
    end

    desc "List types available for a theme"
    task :types, [:theme, :version] do |_t, args|
      theme = args[:theme] || "places"
      version = args[:version]

      types = OvertureMaps::Import::Downloader.list_types(theme: theme, version: version)

      puts "Available types for #{theme}#{version ? " (#{version})" : ""}:"
      types.each { |t| puts "  - #{t}" }
    end

    desc "List files for a theme/type (without downloading)"
    task :list, [:theme, :type, :version, :provider] do |_t, args|
      theme = args[:theme] || "places"
      type = args[:type]
      version = args[:version]
      provider = (args[:provider] || "s3").to_sym

      downloader = OvertureMaps::Import::Downloader.new(
        theme: theme,
        type: type,
        version: version
      )

      puts "Files for #{theme}#{type ? "/#{type}" : ""}#{version ? " (#{version})" : ""}:"
      puts "S3: #{downloader.s3_uri_pattern}"
      puts "Azure: #{downloader.azure_uri_pattern}"
      puts

      files = downloader.list_files(provider: provider)

      if files.any?
        files.each do |f|
          size_mb = (f[:size].to_i / (1024.0 * 1024)).round(2)
          puts "  #{File.basename(f[:key])} (#{size_mb} MB)"
        end
        puts
        puts "Total: #{files.count} file(s)"
      else
        puts "  No files found"
      end
    end

    # Bounding box download (uses DuckDB for server-side filtering)
    desc "Download data for a bounding box (lat1,lng1 = top-left, lat2,lng2 = bottom-right)"
    task :bbox, [:theme, :lat1, :lng1, :lat2, :lng2, :type, :version, :output_dir] do |_t, args|
      download_with_bbox(
        theme: args[:theme] || "places",
        lat1: args[:lat1]&.to_f,
        lng1: args[:lng1]&.to_f,
        lat2: args[:lat2]&.to_f,
        lng2: args[:lng2]&.to_f,
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture"
      )
    end

    # Nearby download (center point + radius)
    desc "Download data near a center point"
    task :nearby, [:theme, :lat, :lng, :radius, :type, :version, :output_dir] do |_t, args|
      download_nearby(
        theme: args[:theme] || "places",
        lat: args[:lat]&.to_f,
        lng: args[:lng]&.to_f,
        radius: args[:radius]&.to_i || 5000,
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture"
      )
    end

    # Download by division name
    desc "Download data for a geographic division (country, state, county, etc.)"
    task :division, [:theme, :division_name, :type, :version, :output_dir] do |_t, args|
      download_by_division(
        theme: args[:theme] || "places",
        division_name: args[:division_name],
        type: args[:type],
        version: args[:version],
        output_dir: args[:output_dir] || "tmp/overture"
      )
    end

    # Search divisions
    desc "Search for geographic divisions"
    task :search_divisions, [:query, :version] do |_t, args|
      search_divisions(
        query: args[:query],
        version: args[:version]
      )
    end
  end
end

def download_files(theme:, type:, version:, output_dir:, provider:)
  require "overture_maps"
  require "overture_maps/import/downloader"

  # Validate theme
  unless OvertureMaps::Import::Downloader::THEMES.include?(theme)
    puts "Error: Unknown theme '#{theme}'"
    puts "Available themes: #{OvertureMaps::Import::Downloader::THEMES.join(", ")}"
    exit 1
  end

  # Validate type if provided
  if type
    valid_types = OvertureMaps::Import::Downloader.types_for_theme(theme)
    unless valid_types.include?(type)
      puts "Error: Unknown type '#{type}' for theme '#{theme}'"
      puts "Available types: #{valid_types.join(", ")}"
      exit 1
    end
  end

  # Create output directory
  FileUtils.mkdir_p(output_dir)

  downloader = OvertureMaps::Import::Downloader.new(
    theme: theme,
    type: type,
    version: version,
    output_dir: output_dir
  )

  puts "Downloading #{theme}#{type ? "/#{type}" : ""}#{version ? " (#{version})" : ""}..."
  puts "Output: #{output_dir}"
  puts "Provider: #{provider}"
  puts

  begin
    case provider
    when :s3
      count = downloader.download_from_s3
    when :azure
      count = downloader.download_from_azure
    else
      raise "Unknown provider: #{provider}"
    end

    puts "\nDownloaded #{count} file(s) successfully!"
  rescue OvertureMaps::Import::Error => e
    puts "Error: #{e.message}"
    exit 1
  end
end

def download_with_bbox(theme:, lat1:, lng1:, lat2:, lng2:, type:, version:, output_dir:)
  require "overture_maps"
  require "overture_maps/import/downloader"

  # Validate required params
  unless lat1 && lng1 && lat2 && lng2
    puts "Error: Missing coordinates"
    puts "Usage: rake overture_maps:download:bbox[theme,lat1,lng1,lat2,lng2]"
    puts "  lat1,lng1 = top-left corner"
    puts "  lat2,lng2 = bottom-right corner"
    puts "Example: rake overture_maps:download:bbox[places,49.5,-125,47,-121]"
    exit 1
  end

  # Validate theme
  unless OvertureMaps::Import::Downloader::THEMES.include?(theme)
    puts "Error: Unknown theme '#{theme}'"
    puts "Available themes: #{OvertureMaps::Import::Downloader::THEMES.join(", ")}"
    exit 1
  end

  FileUtils.mkdir_p(output_dir)

  downloader = OvertureMaps::Import::Downloader.new(
    theme: theme,
    type: type,
    version: version,
    output_dir: output_dir
  )

  puts "Downloading #{theme}#{type ? "/#{type}" : ""} within bounding box..."
  puts "  Top-left:     #{lat1}, #{lng1}"
  puts "  Bottom-right: #{lat2}, #{lng2}"
  puts "  Version:      #{version || "latest"}"
  puts "  Output:       #{output_dir}"
  puts

  begin
    count = downloader.download_from_s3_with_bbox(
      lat1: lat1,
      lng1: lng1,
      lat2: lat2,
      lng2: lng2
    )
    puts "\nDownloaded #{count} file(s) successfully!"
  rescue OvertureMaps::Import::Error => e
    puts "Error: #{e.message}"
    exit 1
  end
end

def download_nearby(theme:, lat:, lng:, radius:, type:, version:, output_dir:)
  require "overture_maps"
  require "overture_maps/import/downloader"

  # Validate required params
  unless lat && lng
    puts "Error: Missing coordinates"
    puts "Usage: rake overture_maps:download:nearby[theme,lat,lng,radius]"
    puts "Example: rake overture_maps:download:nearby[places,40.7128,-74.006,10000]"
    exit 1
  end

  # Validate theme
  unless OvertureMaps::Import::Downloader::THEMES.include?(theme)
    puts "Error: Unknown theme '#{theme}'"
    puts "Available themes: #{OvertureMaps::Import::Downloader::THEMES.join(", ")}"
    exit 1
  end

  FileUtils.mkdir_p(output_dir)

  downloader = OvertureMaps::Import::Downloader.new(
    theme: theme,
    type: type,
    version: version,
    output_dir: output_dir
  )

  puts "Downloading #{theme}#{type ? "/#{type}" : ""} within #{radius}m of #{lat}, #{lng}..."
  puts "  Center:     #{lat}, #{lng}"
  puts "  Radius:     #{radius} meters"
  puts "  Version:    #{version || "latest"}"
  puts "  Output:     #{output_dir}"
  puts

  begin
    count = downloader.download_from_s3_nearby(
      center_lat: lat,
      center_lng: lng,
      radius_meters: radius
    )
    puts "\nDownloaded #{count} file(s) successfully!"
  rescue OvertureMaps::Import::Error => e
    puts "Error: #{e.message}"
    exit 1
  end
end

def download_by_division(theme:, division_name:, type:, version:, output_dir:)
  require "overture_maps"
  require "overture_maps/import/downloader"

  unless division_name
    puts "Error: Missing division name"
    puts "Usage: rake overture_maps:download:division[theme,division_name]"
    puts "Example: rake overture_maps:download:division[places,Washington]"
    puts "         rake overture_maps:download:division[buildings,California]"
    exit 1
  end

  # Validate theme
  unless OvertureMaps::Import::Downloader::THEMES.include?(theme)
    puts "Error: Unknown theme '#{theme}'"
    puts "Available themes: #{OvertureMaps::Import::Downloader::THEMES.join(", ")}"
    exit 1
  end

  FileUtils.mkdir_p(output_dir)

  downloader = OvertureMaps::Import::Downloader.new(
    theme: theme,
    type: type,
    version: version,
    output_dir: output_dir
  )

  puts "Searching for division: #{division_name}"
  puts

  begin
    count = downloader.download_for_division(
      division_name: division_name
    )
    puts "\nDownloaded #{count} file(s) successfully!"
  rescue OvertureMaps::Import::Error => e
    puts "Error: #{e.message}"
    exit 1
  rescue Interrupt
    puts "\nCancelled."
    exit 0
  end
end

def search_divisions(query:, version:)
  require "overture_maps"
  require "overture_maps/import/downloader"

  unless query
    puts "Error: Missing search query"
    puts "Usage: rake overture_maps:download:search_divisions[query]"
    puts "Example: rake overture_maps:download:search_divisions[California]"
    exit 1
  end

  puts "Searching for: #{query}"
  puts

  begin
    results = OvertureMaps::Import::Downloader.search_divisions(query: query, version: version)

    if results.empty?
      puts "No divisions found matching '#{query}'"
    else
      puts "Results:"
      results.each_with_index do |r, i|
        puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]})"
      end
    end
  rescue OvertureMaps::Import::Error => e
    puts "Error: #{e.message}"
    exit 1
  end
end
