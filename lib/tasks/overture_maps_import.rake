# frozen_string_literal: true

require "rake"
require "csv"
require "overture_maps"
require "overture_maps/import/parquet_reader"
require "overture_maps/import/runner"
require "overture_maps/import/location_based_runner"
require "overture_maps/import/downloader"

# Regex to detect if argument looks like coordinates
# Supports: "47.606,-122.336,47.609,-122.333" or "47.606_-122.336_47.609_-122.333"
COORDS_REGEX = /^-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?$/

def parse_location(location)
  return {} unless location

  # Check if it looks like coordinates (4 numbers separated by commas or underscores)
  if location.to_s.match?(/^-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?[,\s]+-?\d+(\.\d+)?$/)
    coords = location.to_s.split(/[,\s]+/).map(&:to_f)
    {
      type: :bbox,
      min_lat: [coords[0], coords[2]].min,
      max_lat: [coords[0], coords[2]].max,
      min_lng: [coords[1], coords[3]].min,
      max_lng: [coords[1], coords[3]].max
    }
  else
    { type: :division_name, name: location }
  end
end

def check_local_file(theme, location_name)
  OvertureMaps::Import::ParquetReader.find_local_file(
    theme: theme,
    location: location_name,
    output_dir: ENV.fetch("OVERTURE_OUTPUT_DIR", "tmp/overture")
  )
end

def prompt_for_local_file(local_file)
  size_mb = (File.size(local_file) / (1024.0 * 1024)).round(2)

  puts "Found local file: #{local_file} (#{size_mb} MB)"
  puts
  puts "Import from this file? (y/n/download)"
  puts "  y        - Import from local file (faster)"
  puts "  n        - Cancel"
  puts "  download - Download fresh data from S3 (may be newer)"
  puts
  print "Enter choice: "

  choice = $stdin.gets&.strip&.downcase

  case choice
  when 'y', 'yes'
    :local
  when 'download', 'd'
    :download
  when 'n', 'no', 'q', 'quit', nil
    :cancel
  else
    puts "Invalid choice. Cancelling."
    :cancel
  end
end

def search_and_select_division(name)
  puts "Searching for: #{name}"
  puts

  results = OvertureMaps::Import::Downloader.search_divisions(query: name)

  if results.empty?
    puts "Error: No divisions found matching '#{name}'"
    puts
    puts "Try searching first to see available options:"
    puts "  rails overture_maps:download:search_divisions[#{name}]"
    exit 1
  end

  if results.count == 1
    result = results.first
    location_info = [result[:country], result[:region]].compact.join(" / ")
    puts "Found: #{result[:name]} (#{result[:subtype]})"
    puts "  Location: #{location_info}" unless location_info.empty?
    puts "  Area: #{result[:area_km2]} km²" if result[:area_km2] && result[:area_km2] > 0
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

    selected = results[idx]
    location_info = [selected[:country], selected[:region]].compact.join(" / ")
    puts "Selected: #{selected[:name]} (#{selected[:subtype]}) - #{location_info}"
    selected
  end
end

def import_from_bbox(theme:, model_class:, min_lat:, max_lat:, min_lng:, max_lng:, categories: nil)
  puts "Importing #{theme} from S3 with spatial filtering..."
  puts "  Bounding box: #{min_lat}, #{min_lng} to #{max_lat}, #{max_lng}"
  puts

  # Ensure DuckDB is available
  OvertureMaps::Import::Downloader.ensure_duckdb_cli!

  # Get types to query
  types = OvertureMaps::Import::Downloader.types_for_theme(theme)

  if types.nil? || types.empty?
    puts "Error: No types found for theme: #{theme}"
    exit 1
  end

  total_imported = 0
  total_errors = 0

  types.each do |type|
    puts "Querying #{theme}/#{type} from S3..."

    # Query S3 for records
    records = OvertureMaps::Import::ParquetReader.query_s3_with_bbox(
      theme: theme,
      type: type,
      min_lat: min_lat,
      max_lat: max_lat,
      min_lng: min_lng,
      max_lng: max_lng
    )

    if records.empty?
      puts "  No data found"
      next
    end

    puts "  Found #{records.length} records, importing..."

    runner = OvertureMaps::Import::Runner.new(
      model_class: model_class,
      batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
    )

    filter = categories ? ->(r) { matches_category?(r, categories) } : nil

    runner.import_from_records(records, filter: filter)

    total_imported += runner.imported_count
    total_errors += runner.error_count

    puts "  Imported: #{runner.imported_count}, Errors: #{runner.error_count}"
  end

  puts
  puts "Import Complete!"
  puts "  Total imported: #{total_imported}"
  puts "  Total errors: #{total_errors}"

  exit(1) if total_errors > 0 && !ENV["IGNORE_ERRORS"]
end

def import_from_local_file(theme:, model_class:, local_file:, categories: nil)
  puts "Importing #{theme} from local file..."
  puts "  File: #{local_file}"
  puts "  Size: #{(File.size(local_file) / (1024.0 * 1024)).round(2)} MB"
  puts

  runner = OvertureMaps::Import::Runner.new(
    model_class: model_class,
    batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
  )

  reader = OvertureMaps::Import::ParquetReader.new(theme: theme)
  filter = categories ? ->(r) { matches_category?(r, categories) } : nil

  runner.import_from_reader(reader, source: local_file, filter: filter)

  puts
  puts "Import Complete!"
  puts "  Imported: #{runner.imported_count}"
  puts "  Errors:   #{runner.error_count}"

  exit(1) unless runner.success?
end

def require_model(model_name)
  begin
    model_class = model_name.constantize

    unless model_class.table_exists?
      puts "Error: #{model_name} table does not exist."
      puts "Please run: rails db:migrate"
      exit 1
    end

    model_class
  rescue NameError
    puts "Error: #{model_name} model not found."
    puts "Please run: rails generate overture_maps:install"
    exit 1
  end
end

def matches_category?(record, categories)
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

namespace :overture_maps do
  namespace :import do
    desc "Import places from Overture Maps (by location name or bounding box)"
    task :places, [:location, :categories] => :environment do |_t, args|
      location = args[:location]
      categories = args[:categories]&.split(",")&.map(&:strip)

      model_class = require_model("OverturePlace")

      if location.nil?
        puts "Error: Missing location argument"
        puts
        puts "Usage:"
        puts "  rails overture_maps:import:places[Seattle]                # Import by location name"
        puts "  rails overture_maps:import:places[47.606,-122.336,47.609,-122.333]  # Import by bbox"
        puts
        puts "Options:"
        puts "  location    - City name, state, country, or bounding box (lat1,lng1,lat2,lng2)"
        puts "  version     - Data version (optional, ignored for location-based import)"
        puts "  categories  - Comma-separated list of categories to filter (optional)"
        puts
        puts "Examples:"
        puts "  rails overture_maps:import:places[Seattle]"
        puts "  rails overture_maps:import:places[California]"
        puts "  rails overture_maps:import:places[47.606,-122.336,47.609,-122.333]"
        puts "  rails overture_maps:import:places[Seattle,,eat_and_drink]"
        exit 1
      end

      parsed = parse_location(location)

      if parsed[:type] == :bbox
        import_from_bbox(
          theme: "places",
          model_class: model_class,
          min_lat: parsed[:min_lat],
          max_lat: parsed[:max_lat],
          min_lng: parsed[:min_lng],
          max_lng: parsed[:max_lng],
          categories: categories
        )
      else
        # Search for division first
        division = search_and_select_division(parsed[:name])

        # Check for local file
        local_file = check_local_file("places", division[:name])

        if local_file
          choice = prompt_for_local_file(local_file)

          case choice
          when :local
            import_from_local_file(
              theme: "places",
              model_class: model_class,
              local_file: local_file,
              categories: categories
            )
          when :cancel
            puts "Cancelled."
            exit 0
          when :download
            puts "Downloading fresh data..."
            # Fall through to S3 import
          end
        end

        # Import from S3 using division's bbox
        bbox = division[:bbox]
        unless bbox
          puts "Error: Could not get bounding box for '#{division[:name]}'"
          exit 1
        end

        import_from_bbox(
          theme: "places",
          model_class: model_class,
          min_lat: bbox["ymin"],
          max_lat: bbox["ymax"],
          min_lng: bbox["xmin"],
          max_lng: bbox["xmax"],
          categories: categories
        )
      end
    end

    desc "Import buildings from Overture Maps (by location name or bounding box)"
    task :buildings, [:location] => :environment do |_t, args|
      location = args[:location]

      model_class = require_model("OvertureBuilding")

      if location.nil?
        puts "Error: Missing location argument"
        puts
        puts "Usage:"
        puts "  rails overture_maps:import:buildings[Seattle]"
        puts "  rails overture_maps:import:buildings[47.606,-122.336,47.609,-122.333]"
        exit 1
      end

      parsed = parse_location(location)

      if parsed[:type] == :bbox
        import_from_bbox(
          theme: "buildings",
          model_class: model_class,
          min_lat: parsed[:min_lat],
          max_lat: parsed[:max_lat],
          min_lng: parsed[:min_lng],
          max_lng: parsed[:max_lng]
        )
      else
        division = search_and_select_division(parsed[:name])
        local_file = check_local_file("buildings", division[:name])

        if local_file
          choice = prompt_for_local_file(local_file)

          case choice
          when :local
            import_from_local_file(
              theme: "buildings",
              model_class: model_class,
              local_file: local_file
            )
            next
          when :cancel
            puts "Cancelled."
            exit 0
          when :download
            puts "Downloading fresh data..."
          end
        end

        bbox = division[:bbox]
        unless bbox
          puts "Error: Could not get bounding box for '#{division[:name]}'"
          exit 1
        end

        import_from_bbox(
          theme: "buildings",
          model_class: model_class,
          min_lat: bbox["ymin"],
          max_lat: bbox["ymax"],
          min_lng: bbox["xmin"],
          max_lng: bbox["xmax"]
        )
      end
    end

    desc "Import addresses from Overture Maps (by location name or bounding box)"
    task :addresses, [:location] => :environment do |_t, args|
      location = args[:location]

      model_class = require_model("OvertureAddress")

      if location.nil?
        puts "Error: Missing location argument"
        puts
        puts "Usage:"
        puts "  rails overture_maps:import:addresses[Seattle]"
        puts "  rails overture_maps:import:addresses[47.606,-122.336,47.609,-122.333]"
        exit 1
      end

      parsed = parse_location(location)

      if parsed[:type] == :bbox
        import_from_bbox(
          theme: "addresses",
          model_class: model_class,
          min_lat: parsed[:min_lat],
          max_lat: parsed[:max_lat],
          min_lng: parsed[:min_lng],
          max_lng: parsed[:max_lng]
        )
      else
        division = search_and_select_division(parsed[:name])
        local_file = check_local_file("addresses", division[:name])

        if local_file
          choice = prompt_for_local_file(local_file)

          case choice
          when :local
            import_from_local_file(
              theme: "addresses",
              model_class: model_class,
              local_file: local_file
            )
            next
          when :cancel
            puts "Cancelled."
            exit 0
          when :download
            puts "Downloading fresh data..."
          end
        end

        bbox = division[:bbox]
        unless bbox
          puts "Error: Could not get bounding box for '#{division[:name]}'"
          exit 1
        end

        import_from_bbox(
          theme: "addresses",
          model_class: model_class,
          min_lat: bbox["ymin"],
          max_lat: bbox["ymax"],
          min_lng: bbox["xmin"],
          max_lng: bbox["xmax"]
        )
      end
    end

    desc "Import all themes for a location"
    task :all, [:location] => :environment do |_t, args|
      location = args[:location]

      if location.nil?
        puts "Error: Missing location argument"
        puts
        puts "Usage:"
        puts "  rails overture_maps:import:all[Seattle]"
        puts "  rails overture_maps:import:all[47.606_-122.336_47.609_-122.333]"
        exit 1
      end

      # Import each theme in sequence
      %w[places buildings addresses].each do |theme|
        puts "=" * 60
        puts "Importing #{theme}..."
        puts "=" * 60
        puts

        task_name = "overture_maps:import:#{theme}"
        Rake::Task[task_name].invoke(location)
        Rake::Task[task_name].reenable

        puts
      end

      puts "All imports complete!"
    end

    desc "Show import statistics"
    task :stats => :environment do |_t, _args|
      puts "Import Statistics:"
      puts "  Places:     #{OverturePlace.count rescue 'N/A (run `rails generate overture_maps:install` and `rails db:migrate`)'}"
      puts "  Buildings:  #{OvertureBuilding.count rescue 'N/A'}"
      puts "  Addresses:  #{OvertureAddress.count rescue 'N/A'}"
    end

    desc "List available versions"
    task :versions do |_t, _args|
      versions = OvertureMaps::Import::Downloader.list_versions

      puts "Available versions:"
      versions.each { |v| puts "  - #{v}" }

      puts
      puts "Latest: #{OvertureMaps::Import::Downloader.latest_version}"
    end

    desc "Search for geographic divisions (for use with import)"
    task :search, [:query] => :environment do |_t, args|
      query = args[:query]

      unless query
        puts "Error: Missing search query"
        puts "Usage: rails overture_maps:import:search[Seattle]"
        exit 1
      end

      puts "Searching for: #{query}"
      puts

      begin
        OvertureMaps::Import::Downloader.ensure_duckdb_cli!
        results = OvertureMaps::Import::Downloader.search_divisions(query: query)

        if results.empty?
          puts "No divisions found matching '#{query}'"
        else
          puts "Results:"
          results.each_with_index do |r, i|
            location_parts = [r[:country], r[:region]].compact
            location = location_parts.any? ? " - #{location_parts.join(" / ")}" : ""
            pop_info = r[:population] ? " (#{r[:population].to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} people)" : ""
            area_info = r[:area_km2] && r[:area_km2] > 0 ? " [#{r[:area_km2]} km²]" : ""
            puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]})#{location}#{pop_info}#{area_info}"
          end
          puts
          puts "Tip: Use the name with import tasks, e.g.:"
          puts "  rails overture_maps:import:places[#{results.first[:name]}]"
        end
      rescue OvertureMaps::Import::Error => e
        puts "Error: #{e.message}"
        exit 1
      end
    end
  end
end

namespace :overture_maps do
  namespace :categories do
    desc "Populate categories from Overture Maps taxonomy"
    task :populate => :environment do
      require_model("OvertureCategory")

      puts "Fetching categories from Overture Maps taxonomy..."

      url = "https://raw.githubusercontent.com/OvertureMaps/schema/main/docs/schema/concepts/by-theme/places/overture_categories.csv"

      require "net/http"

      response = Net::HTTP.get(URI(url))
      raise "Failed to fetch categories" unless response

      csv = CSV.new(response, headers: false, col_sep: ";")
      count = 0

      csv.each do |row|
        next if row.empty?

        name = row[0]&.strip
        taxonomy_str = row[1]&.strip

        next if name.nil? || name.empty?

        taxonomy = taxonomy_str.gsub(/[\[\]]/, "").split(",").map(&:strip)
        primary = taxonomy.first
        hierarchy_level = taxonomy.size - 1

        OvertureCategory.find_or_create_by!(name: name) do |c|
          c.primary_category = primary
          c.hierarchy_level = hierarchy_level
        end
        count += 1
      end

      puts "Imported #{count} categories!"
      puts "\nPrimary categories available:"
      OvertureCategory.distinct.pluck(:primary_category).compact.sort.each do |pc|
        puts "  - #{pc}"
      end
    end

    desc "List all categories"
    task :list => :environment do
      require_model("OvertureCategory")

      puts "Primary categories:"
      OvertureCategory.distinct.pluck(:primary_category).compact.sort.each do |pc|
        puts "  - #{pc}"
        OvertureCategory.where(primary_category: pc).order(:hierarchy_level, :name).each do |cat|
          puts "      #{cat.name}"
        end
      end
    end

    desc "List primary categories only"
    task :primary => :environment do
      require_model("OvertureCategory")

      OvertureCategory.distinct.pluck(:primary_category).compact.sort.each do |pc|
        puts pc
      end
    end
  end
end
