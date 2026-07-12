# frozen_string_literal: true

require "overture_maps"

module OvertureMaps
  # Interactive glue for the rake tasks. Prompts and process exits live here
  # and only here — the library classes raise or take callbacks instead.
  module RakeUI
    # Which feature types are imported per theme, and into which model.
    # building_part is skipped for now (needs a building_id relationship).
    IMPORT_THEMES = {
      "places" => { "place" => "OverturePlace" },
      "buildings" => { "building" => "OvertureBuilding" },
      "addresses" => { "address" => "OvertureAddress" },
      "divisions" => { "division_area" => "OvertureDivision" },
      "transportation" => {
        "segment" => "OvertureSegment",
        "connector" => "OvertureConnector"
      },
      "base" => OvertureMaps::Import::Downloader::TYPES["base"]
              .to_h { |t| [t, "OvertureBaseFeature"] }
    }.freeze

    module_function

    def interactive?
      !OvertureMaps.configuration.non_interactive && $stdin.tty?
    end

    def abort!(message)
      puts message
      exit 1
    end

    # Resolves { type => model_name } to { type => model_class }, verifying
    # tables exist.
    def require_models!(theme)
      mapping = IMPORT_THEMES[theme] or abort!("No import models for theme #{theme}")
      mapping.to_h do |type, name|
        model = begin
          name.constantize
        rescue NameError
          abort!("#{name} model not found. Run: rails generate overture_maps:install")
        end
        abort!("#{name} table does not exist. Run: rails db:migrate") unless model.table_exists?
        [type, model]
      end
    end

    def select_division_callback
      return nil unless interactive?

      ->(results) { prompt_division(results) }
    end

    def confirm_cached_callback
      return nil unless interactive?

      ->(path) { prompt_cached(path) }
    end

    def prompt_division(results)
      puts "Multiple matches found:"
      results.each_with_index do |r, i|
        location_info = [r[:country], r[:region]].compact.join(" / ")
        area_info = r[:area_km2]&.positive? ? " (#{r[:area_km2]} km²)" : ""
        puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]}) - #{location_info}#{area_info}"
      end
      puts
      print "Enter number to select (or 'q' to quit): "
      input = $stdin.gets&.strip

      return nil if input.nil? || input.downcase == "q"

      index = input.to_i - 1
      return nil unless index >= 0 && index < results.length

      results[index]
    end

    def prompt_cached(path)
      size = OvertureMaps::Util.format_size(File.size(path))
      puts "Found cached extract: #{path} (#{size})"
      print "Use it? (y = use / d = download fresh / q = quit): "

      case $stdin.gets&.strip&.downcase
      when "y", "yes", "", nil then :use
      when "d", "download" then :refresh
      else :abort
      end
    end

    # Resolves a location argument (bbox string or division name) to a
    # BoundingBox for the download tasks.
    def resolve_bbox(location, release: nil)
      bbox = OvertureMaps::BoundingBox.parse(location)
      return bbox if bbox

      results = OvertureMaps::DivisionSearch.search(query: location, release: release)
      abort!("No divisions found matching '#{location}'") if results.empty?

      division = results.length == 1 || !interactive? ? results.first : prompt_division(results)
      if division.nil?
        puts "Cancelled."
        exit 0
      end

      info = [division[:country], division[:region]].compact.join(" / ")
      puts "Using #{division[:name]} (#{division[:subtype]}#{info.empty? ? "" : ", #{info}"})"
      division[:bbox]
    end

    def print_divisions(results)
      results.each_with_index do |r, i|
        location_parts = [r[:country], r[:region]].compact
        location = location_parts.any? ? " - #{location_parts.join(" / ")}" : ""
        area_info = r[:area_km2]&.positive? ? " [#{r[:area_km2]} km²]" : ""
        puts "  #{i + 1}. #{r[:name]} (#{r[:subtype]})#{location}#{area_info}"
      end
    end

    def release_arg
      ENV["OVERTURE_RELEASE"]
    end
  end
end

namespace :overture_maps do
  namespace :import do
    OvertureMaps::RakeUI::IMPORT_THEMES.each_key do |theme|
      desc "Import #{theme} by location name or bounding box, e.g. rails overture_maps:import:#{theme}[Seattle]"
      task theme.to_sym, [:location, :categories] => :environment do |_t, args|
        location = args[:location]
        unless location
          puts "Usage:"
          puts "  rails overture_maps:import:#{theme}[Seattle]"
          puts "  rails \"overture_maps:import:#{theme}[47.606_-122.336_47.609_-122.333]\""
          puts "  rails overture_maps:import:#{theme}[Seattle,\"cafe,restaurant\"]  # categories or groups" if theme == "places"
          exit 1
        end

        categories = args[:categories]&.split(",")&.map(&:strip)
        models = OvertureMaps::RakeUI.require_models!(theme)

        begin
          runner = OvertureMaps::Import::LocationBasedRunner.new(
            theme: theme,
            location: location,
            models: models,
            categories: categories,
            release: OvertureMaps::RakeUI.release_arg,
            select_division: OvertureMaps::RakeUI.select_division_callback,
            confirm_cached: OvertureMaps::RakeUI.confirm_cached_callback
          ).run
        rescue OvertureMaps::CancelledError
          puts "Cancelled."
          exit 0
        rescue OvertureMaps::Error => e
          OvertureMaps::RakeUI.abort!("Error: #{e.message}")
        end

        puts
        puts "Import complete: #{runner.imported_count} imported, #{runner.error_count} errors"
        if runner.errors.any? && ENV["VERBOSE"]
          runner.errors.first(10).each { |err| puts "  - #{err[:error]} (#{err[:record_id]})" }
        end
        exit 1 if runner.error_count.positive? && !ENV["IGNORE_ERRORS"]
      end
    end

    desc "Import all themes for a location"
    task :all, [:location] => :environment do |_t, args|
      location = args[:location] or OvertureMaps::RakeUI.abort!(
        "Usage: rails overture_maps:import:all[Seattle]"
      )

      # Resolve the location once so each theme reuses the same bbox instead
      # of re-searching (and re-prompting).
      bbox = OvertureMaps::RakeUI.resolve_bbox(location, release: OvertureMaps::RakeUI.release_arg)

      failures = 0
      OvertureMaps::RakeUI::IMPORT_THEMES.each_key do |theme|
        puts "=" * 60
        puts "Importing #{theme}..."
        puts "=" * 60

        models = OvertureMaps::RakeUI.require_models!(theme)
        runner = OvertureMaps::Import::LocationBasedRunner.new(
          theme: theme, location: bbox, models: models,
          release: OvertureMaps::RakeUI.release_arg,
          confirm_cached: OvertureMaps::RakeUI.confirm_cached_callback
        ).run
        puts "#{theme}: #{runner.imported_count} imported, #{runner.error_count} errors"
        failures += runner.error_count
        puts
      end

      puts "All imports complete!"
      exit 1 if failures.positive? && !ENV["IGNORE_ERRORS"]
    end

    desc "Search for geographic divisions"
    task :search, [:query] => :environment do |_t, args|
      query = args[:query] or OvertureMaps::RakeUI.abort!(
        "Usage: rails overture_maps:import:search[Seattle]"
      )

      results = OvertureMaps::DivisionSearch.search(
        query: query, release: OvertureMaps::RakeUI.release_arg
      )
      if results.empty?
        puts "No divisions found matching '#{query}'"
      else
        puts "Results:"
        OvertureMaps::RakeUI.print_divisions(results)
        puts
        puts "Tip: rails overture_maps:import:places[#{results.first[:name]}]"
      end
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "Show import statistics"
    task stats: :environment do
      OvertureMaps::RakeUI::IMPORT_THEMES.values.flat_map(&:values).uniq.each do |model_name|
        count = begin
          model_name.constantize.count
        rescue StandardError
          "N/A (run rails generate overture_maps:install && rails db:migrate)"
        end
        puts format("  %-24s %s", "#{model_name}:", count)
      end
    end
  end

  namespace :download do
    OvertureMaps::Import::Downloader::THEMES.each do |theme|
      desc "Download #{theme} data by location name or bbox; without a location downloads complete theme files (large!)"
      task theme.to_sym, [:location, :release, :output_dir] do |_t, args|
        release = args[:release] || OvertureMaps::RakeUI.release_arg
        downloader = OvertureMaps::Import::Downloader.new(
          theme: theme, release: release, output_dir: args[:output_dir]
        )

        begin
          if args[:location]
            bbox = OvertureMaps::RakeUI.resolve_bbox(args[:location], release: release)
            files = downloader.extract_bbox_all_types(bbox)
            puts "\nDownloaded #{files.count} extract(s)"
          else
            puts "No location given — downloading ALL #{theme} files for #{downloader.release}."
            puts "This can be very large; prefer overture_maps:download:#{theme}[<location>]."
            count = downloader.download_theme_files
            puts "\nDownloaded #{count} file(s)"
          end
        rescue OvertureMaps::Error => e
          OvertureMaps::RakeUI.abort!("Error: #{e.message}")
        end
      end
    end

    desc "Download data within a bounding box, e.g. rails overture_maps:download:bbox[places,47.6,-122.4,47.7,-122.2]"
    task :bbox, [:theme, :lat1, :lng1, :lat2, :lng2, :type, :release, :output_dir, :format] do |_t, args|
      unless args[:lat1] && args[:lng1] && args[:lat2] && args[:lng2]
        OvertureMaps::RakeUI.abort!("Usage: rails overture_maps:download:bbox[theme,lat1,lng1,lat2,lng2]")
      end

      bbox = OvertureMaps::BoundingBox.new(
        lat1: args[:lat1], lng1: args[:lng1], lat2: args[:lat2], lng2: args[:lng2]
      )
      downloader = OvertureMaps::Import::Downloader.new(
        theme: args[:theme] || "places", type: args[:type],
        release: args[:release] || OvertureMaps::RakeUI.release_arg,
        output_dir: args[:output_dir]
      )
      files = downloader.extract_bbox_all_types(bbox, format: args[:format] || "parquet")
      puts "\nDownloaded #{files.count} extract(s)"
    rescue OvertureMaps::Error, ArgumentError => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "Download data near a point, e.g. rails overture_maps:download:nearby[places,47.6,-122.3,5000]"
    task :nearby, [:theme, :lat, :lng, :radius, :type, :release, :output_dir] do |_t, args|
      unless args[:lat] && args[:lng]
        OvertureMaps::RakeUI.abort!("Usage: rails overture_maps:download:nearby[theme,lat,lng,radius_meters]")
      end

      downloader = OvertureMaps::Import::Downloader.new(
        theme: args[:theme] || "places", type: args[:type],
        release: args[:release] || OvertureMaps::RakeUI.release_arg,
        output_dir: args[:output_dir]
      )
      files = downloader.extract_nearby(
        lat: args[:lat], lng: args[:lng],
        radius_meters: (args[:radius] || 5000).to_i
      )
      puts "\nDownloaded #{files.count} extract(s)"
    rescue OvertureMaps::Error, ArgumentError => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "Search for geographic divisions"
    task :search_divisions, [:query] do |_t, args|
      query = args[:query] or OvertureMaps::RakeUI.abort!(
        "Usage: rails overture_maps:download:search_divisions[query]"
      )

      results = OvertureMaps::Import::Downloader.search_divisions(
        query: query, release: OvertureMaps::RakeUI.release_arg
      )
      if results.empty?
        puts "No divisions found matching '#{query}'"
      else
        puts "Results:"
        OvertureMaps::RakeUI.print_divisions(results)
      end
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "List available Overture releases"
    task :versions do
      releases = OvertureMaps::Releases.all
      puts "Available releases:"
      releases.each_with_index do |release, i|
        puts "  - #{release}#{i.zero? ? "  (latest)" : ""}"
      end
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "List available themes and their types"
    task :themes do
      OvertureMaps::Import::Downloader.themes_with_types.each do |theme, types|
        puts "  - #{theme}"
        types.each { |t| puts "      #{t}" }
      end
    end

    desc "List types available for a theme in the current release"
    task :types, [:theme] do |_t, args|
      theme = args[:theme] || "places"
      types = OvertureMaps::Import::Downloader.list_types(
        theme: theme, release: OvertureMaps::RakeUI.release_arg
      )
      puts "Available types for #{theme}:"
      types.each { |t| puts "  - #{t}" }
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "List files for a theme/type without downloading"
    task :list, [:theme, :type, :release] do |_t, args|
      downloader = OvertureMaps::Import::Downloader.new(
        theme: args[:theme] || "places", type: args[:type],
        release: args[:release] || OvertureMaps::RakeUI.release_arg
      )
      files = downloader.list_files
      if files.empty?
        puts "No files found"
      else
        files.each do |f|
          puts "  #{File.basename(f[:key])} (#{OvertureMaps::Util.format_size(f[:size])})"
        end
        puts "Total: #{files.count} file(s)"
      end
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end
  end

  desc "Sync imported areas to the latest (or given) Overture release"
  task :sync, [:release] => :environment do |_t, args|
    unless OvertureMaps::Models::ImportedArea.table_exists?
      OvertureMaps::RakeUI.abort!(
        "No sync tracking table. Run: rails generate overture_maps:install && rails db:migrate\n" \
        "(Areas are tracked from their next import.)"
      )
    end

    syncer = OvertureMaps::Syncer.new(target_release: args[:release])
    areas = OvertureMaps::Models::ImportedArea.count
    if areas.zero?
      puts "No imported areas tracked yet. Run an import first."
      next
    end

    puts "Syncing #{areas} imported area(s) to #{syncer.target}..."
    puts

    failures = 0
    syncer.sync_all.each do |result|
      area = result.area
      label = "#{area.theme}/#{area.feature_type} #{area.slug}"
      case result.status
      when :up_to_date
        puts "  #{label}: already at #{syncer.target}"
      when :synced
        puts "  #{label}: synced (removed #{result.removed}, upserted #{result.imported}, errors #{result.errors})"
      when :refreshed
        puts "  #{label}: fully refreshed (#{result.message})"
      when :failed
        puts "  #{label}: FAILED — #{result.message}"
        failures += 1
      end
      failures += 1 if result.errors.positive? && result.status != :failed
    end

    puts
    puts failures.zero? ? "Sync complete." : "Sync finished with #{failures} failure(s)."
    exit 1 if failures.positive? && !ENV["IGNORE_ERRORS"]
  end

  namespace :sync do
    desc "Show tracked areas and their releases"
    task status: :environment do
      unless OvertureMaps::Models::ImportedArea.table_exists?
        OvertureMaps::RakeUI.abort!("No sync tracking table. Run: rails generate overture_maps:install && rails db:migrate")
      end

      latest = OvertureMaps::Releases.latest
      puts "Latest Overture release: #{latest}"
      puts

      areas = OvertureMaps::Models::ImportedArea.order(:theme, :feature_type, :slug)
      if areas.none?
        puts "No imported areas tracked yet."
      else
        areas.each do |area|
          marker = area.release == latest ? "  " : "! "
          puts "#{marker}#{area.theme}/#{area.feature_type} #{area.slug}: " \
               "#{area.release} (#{area.records_count} records)"
        end
        behind = areas.count { |a| a.release != latest }
        puts
        puts behind.zero? ? "All areas up to date." : "#{behind} area(s) behind — run rails overture_maps:sync"
      end
    end
  end

  namespace :gers do
    desc "Look up a GERS id in the Overture registry"
    task :lookup, [:id] do |_t, args|
      id = args[:id] or OvertureMaps::RakeUI.abort!("Usage: rails overture_maps:gers:lookup[<gers-id>]")

      begin
        row = OvertureMaps::GERS.lookup(id)
        if row
          row.each { |key, value| puts format("  %-14s %s", "#{key}:", value) }
        else
          puts "Not found in the registry."
        end
      rescue ArgumentError => e
        OvertureMaps::RakeUI.abort!("Error: #{e.message}")
      rescue OvertureMaps::Error => e
        OvertureMaps::RakeUI.abort!("Error: #{e.message}")
      end
    end
  end

  namespace :cache do
    desc "List cached extracts"
    task :list do
      dir = OvertureMaps.configuration.cache_dir
      files = Dir.glob(File.join(dir, "*.{parquet,geojson,geojsonseq,gpkg}")).sort

      if files.empty?
        puts "No cached extracts in #{dir}"
      else
        total = 0
        files.each do |f|
          size = File.size(f)
          total += size
          puts "  #{File.basename(f)} (#{OvertureMaps::Util.format_size(size)})"
        end
        puts "Total: #{files.count} file(s), #{OvertureMaps::Util.format_size(total)}"
      end
    end

    desc "Remove cached extracts (optionally matching a pattern)"
    task :clear, [:pattern] do |_t, args|
      dir = OvertureMaps.configuration.cache_dir
      pattern = args[:pattern] ? "*#{args[:pattern]}*" : "*"
      files = Dir.glob(File.join(dir, "#{pattern}.{parquet,geojson,geojsonseq,gpkg}"))

      if files.empty?
        puts "Nothing to remove"
      else
        files.each { |f| File.delete(f) }
        puts "Removed #{files.count} file(s)"
      end
    end
  end

  namespace :categories do
    CATEGORIES_CSV_URL = "https://raw.githubusercontent.com/OvertureMaps/schema/main/docs/schema/concepts/by-theme/places/overture_categories.csv"

    desc "Populate the categories taxonomy from the Overture schema repo"
    task populate: :environment do
      require "csv"

      begin
        OvertureCategory
      rescue NameError
        OvertureMaps::RakeUI.abort!("OvertureCategory model not found. Run: rails generate overture_maps:install")
      end

      puts "Fetching categories from the Overture Maps taxonomy..."
      body = OvertureMaps::Storage.get(CATEGORIES_CSV_URL)

      count = 0
      skipped = 0
      CSV.parse(body, headers: true, col_sep: ";").each do |row|
        name = row[0]&.strip
        taxonomy_str = row[1]&.strip

        if name.nil? || name.empty? || taxonomy_str.nil? || taxonomy_str.empty?
          skipped += 1 unless name.nil? && taxonomy_str.nil?
          next
        end

        taxonomy = taxonomy_str.gsub(/[\[\]]/, "").split(",").map(&:strip)
        category = OvertureCategory.find_or_initialize_by(name: name)
        category.update!(
          primary_category: taxonomy.first,
          hierarchy_level: taxonomy.size - 1,
          taxonomy: taxonomy
        )
        count += 1
      end

      puts "Imported #{count} categories#{skipped.positive? ? " (skipped #{skipped} malformed rows)" : ""}."
      puts "\nPrimary categories:"
      OvertureCategory.primary_categories.each { |pc| puts "  - #{pc}" }
    rescue OvertureMaps::Error => e
      OvertureMaps::RakeUI.abort!("Error: #{e.message}")
    end

    desc "List all categories grouped by primary category"
    task list: :environment do
      OvertureCategory.primary_categories.each do |pc|
        puts "  - #{pc}"
        OvertureCategory.by_primary(pc).order(:hierarchy_level, :name).each do |cat|
          puts "      #{cat.name}"
        end
      end
    end

    desc "List primary categories only"
    task primary: :environment do
      OvertureCategory.primary_categories.each { |pc| puts pc }
    end
  end
end
