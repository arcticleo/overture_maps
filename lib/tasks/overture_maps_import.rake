# frozen_string_literal: true

require "rake"
require "csv"
require "overture_maps"
require "overture_maps/import/parquet_reader"
require "overture_maps/import/runner"

namespace :overture_maps do
  namespace :import do
    desc "Import places from Overture Maps Parquet files"
    task :places, [:region, :source, :categories] => :environment do |_t, args|
      region = args[:region]
      source = args[:source]&.to_sym || :s3
      categories = args[:categories]&.split(",")&.map(&:strip)

      require_model("OverturePlace")

      puts "Importing places#{region ? " for region: #{region}" : ""} from #{source}..."
      puts "Filtering by categories: #{categories.join(", ")}" if categories

      reader = OvertureMaps::Import::ParquetReader.new(
        theme: "places",
        region: region
      )

      runner = OvertureMaps::Import::Runner.new(
        model_class: OverturePlace,
        batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
      )

      filter = categories ? ->(record) { matches_category?(record, categories) } : nil

      runner.import_from_reader(reader, source: source, filter: filter) do |record|
        transform_place_record(record)
      end

      print_results(runner)
    end

    desc "Import buildings from Overture Maps Parquet files"
    task :buildings, [:region, :source] => :environment do |_t, args|
      region = args[:region]
      source = args[:source]&.to_sym || :s3

      require_model("OvertureBuilding")

      puts "Importing buildings#{region ? " for region: #{region}" : ""} from #{source}..."

      reader = OvertureMaps::Import::ParquetReader.new(
        theme: "buildings",
        region: region
      )

      runner = OvertureMaps::Import::Runner.new(
        model_class: OvertureBuilding,
        batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
      )

      runner.import_from_reader(reader, source: source) do |record|
        transform_building_record(record)
      end

      print_results(runner)
    end

    desc "Import addresses from Overture Maps Parquet files"
    task :addresses, [:region, :source] => :environment do |_t, args|
      region = args[:region]
      source = args[:source]&.to_sym || :s3

      require_model("OvertureAddress")

      puts "Importing addresses#{region ? " for region: #{region}" : ""} from #{source}..."

      reader = OvertureMaps::Import::ParquetReader.new(
        theme: "addresses",
        region: region
      )

      runner = OvertureMaps::Import::Runner.new(
        model_class: OvertureAddress,
        batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
      )

      runner.import_from_reader(reader, source: source) do |record|
        transform_address_record(record)
      end

      print_results(runner)
    end

    desc "Import all themes for a region"
    task :all, [:region, :source] => :environment do |_t, args|
      region = args[:region]
      source = args[:source]&.to_sym || :s3

      %w[places buildings addresses].each do |theme|
        Rake::Task["overture_maps:import:#{theme}"].invoke(region, source)
        Rake::Task["overture_maps:import:#{theme}"].reenable
      end
    end

    desc "List available regions for a theme"
    task :regions, [:theme] do |_t, args|
      theme = args[:theme] || "places"

      regions = OvertureMaps::Import::ParquetReader.list_regions(theme: theme)

      puts "Available regions for #{theme}:"
      regions.each { |r| puts "  - #{r}" }
    end

    desc "List available versions"
    task :versions do |_t, _args|
      versions = OvertureMaps::Import::ParquetReader.list_versions

      puts "Available versions:"
      versions.each { |v| puts "  - #{v}" }
    end

    desc "Show import statistics"
    task :stats => :environment do |_t, _args|
      puts "Import Statistics:"
      puts "  Places:     #{OverturePlace.count rescue 'N/A'}"
      puts "  Buildings:   #{OvertureBuilding.count rescue 'N/A'}"
      puts "  Addresses:  #{OvertureAddress.count rescue 'N/A'}"
    end
  end
end

def require_model(model_name)
  begin
    # Use Rails' constantize to trigger autoloading
    model_class = model_name.constantize

    # Also check if the table exists
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

  exit 1 unless runner.success?
end

def matches_category?(record, categories)
  record_categories = record["categories"]
  return false unless record_categories

  # categories can be a JSON string or hash
  cat_hash = case record_categories
             when String then JSON.parse(record_categories)
             when Hash then record_categories
             else return false
             end

  return false unless cat_hash.is_a?(Hash)

  # Check if any of the requested categories match
  # Can match primary category (key) or any sub-category (value)
  categories.any? do |wanted|
    # Check if it's a primary category
    cat_hash.key?(wanted) ||
      # Check if it's a sub-category
      cat_hash.values.any? { |v| Array(v).include?(wanted) }
  end
end

def transform_place_record(record)
  {
    id: record["id"],
    names: record["names"],
    categories: record["categories"],
    brands: record["brands"],
    addresses: record["addresses"],
    confidence: record["confidence"],
    elevation: record["elevation"],
    country: record["country"],
    geometry: parse_wkb_geometry(record["geometry"]),
    created_at: Time.current,
    updated_at: Time.current
  }
end

def transform_building_record(record)
  {
    id: record["id"],
    names: record["names"],
    height: record["height"],
    level: record["level"],
    class: record["class"],
    is_underground: record["is_underground"],
    geometry: parse_wkb_geometry(record["geometry"]),
    created_at: Time.current,
    updated_at: Time.current
  }
end

def transform_address_record(record)
  {
    id: record["id"],
    street: record["street"],
    locality: record["locality"],
    region: record["region"],
    country: record["country"],
    postcode: record["postcode"],
    geometry: parse_wkb_geometry(record["geometry"]),
    created_at: Time.current,
    updated_at: Time.current
  }
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

        # Parse taxonomy to extract primary category and hierarchy level
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

def parse_wkb_geometry(geom)
  return nil unless geom

  factory = RGeo::Geographic.spherical_factory(srid: 4326)

  case geom
  when String
    factory.parse_wkb(geom)
  when Hash
    RGeo::GeoJSON.decode(geom, geo_factory: factory)
  else
    nil
  end
end
