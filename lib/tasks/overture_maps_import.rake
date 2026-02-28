# frozen_string_literal: true

require "rake"
require "overture_maps"
require "overture_maps/import/parquet_reader"
require "overture_maps/import/runner"

namespace :overture_maps do
  namespace :import do
    desc "Import places from Overture Maps Parquet files"
    task :places, [:region, :source] do |_t, args|
      region = args[:region]
      source = args[:source]&.to_sym || :s3

      require_model("OverturePlace")

      puts "Importing places#{region ? " for region: #{region}" : ""} from #{source}..."

      reader = OvertureMaps::Import::ParquetReader.new(
        theme: "places",
        region: region
      )

      runner = OvertureMaps::Import::Runner.new(
        model_class: OverturePlace,
        batch_size: ENV.fetch("BATCH_SIZE", 1000).to_i
      )

      runner.import_from_reader(reader, source: source) do |record|
        transform_place_record(record)
      end

      print_results(runner)
    end

    desc "Import buildings from Overture Maps Parquet files"
    task :buildings, [:region, :source] do |_t, args|
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
    task :addresses, [:region, :source] do |_t, args|
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
    task :all, [:region, :source] do |_t, args|
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
    task :stats do |_t, _args|
      puts "Import Statistics:"
      puts "  Places:     #{OverturePlace.count rescue 'N/A'}"
      puts "  Buildings:   #{OvertureBuilding.count rescue 'N/A'}"
      puts "  Addresses:  #{OvertureAddress.count rescue 'N/A'}"
    end
  end
end

def require_model(model_name)
  begin
    Object.const_get(model_name)
  rescue NameError
    puts "Error: #{model_name} model not found."
    puts "Please run: rails generate overture_maps:#{model_name.underscore}"
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
