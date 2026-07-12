# frozen_string_literal: true

require "fileutils"

module OvertureMaps
  module Import
    # Downloads Overture data: whole theme files via anonymous HTTP, and
    # bbox-filtered extracts via DuckDB (which pushes the bbox predicate down
    # to parquet row-group statistics, so only the relevant slice of the
    # dataset is transferred).
    class Downloader
      THEMES = %w[addresses base buildings divisions places transportation].freeze

      TYPES = {
        "addresses" => %w[address],
        "base" => %w[bathymetry infrastructure land land_cover land_use water],
        "buildings" => %w[building building_part],
        "divisions" => %w[division division_area division_boundary],
        "places" => %w[place],
        "transportation" => %w[connector segment]
      }.freeze

      # division_area subtypes worth surfacing in a name search, roughly
      # largest to smallest.
      DIVISION_SUBTYPES = %w[country dependency region county localadmin locality borough neighborhood].freeze

      EXTRACT_FORMATS = {
        "parquet" => "parquet",
        "geojson" => "geojson",
        "geojsonseq" => "geojsonseq",
        "gpkg" => "gpkg",
        "geopackage" => "gpkg"
      }.freeze

      attr_reader :theme, :type, :release, :output_dir

      def initialize(theme:, type: nil, release: nil, output_dir: nil)
        raise ArgumentError, "unknown theme: #{theme}" unless THEMES.include?(theme)
        raise ArgumentError, "unknown type #{type} for #{theme}" if type && !TYPES[theme].include?(type)

        @theme = theme
        @type = type
        @release = Releases.validate!(release || Releases.current)
        @output_dir = output_dir || OvertureMaps.configuration.cache_dir
      end

      def self.types_for_theme(theme)
        TYPES[theme] || []
      end

      def self.themes_with_types
        TYPES
      end

      # Downloads the complete parquet files for this theme/type. These are
      # large (buildings alone is hundreds of GB globally) — bbox extracts
      # are almost always what you want instead.
      def download_theme_files
        files = list_files
        if files.empty?
          log "No files found for #{theme}#{type ? "/#{type}" : ""} in #{release}"
          return 0
        end

        log "Found #{files.count} file(s) to download..."
        FileUtils.mkdir_p(output_dir)

        files.each do |file|
          filename = File.basename(file[:key])
          local_path = File.join(output_dir, filename)

          result = Storage.download(file[:key], to: local_path, expected_size: file[:size])
          if result == :skipped
            log "Skipping #{filename} (already exists)"
          else
            log "Downloaded #{filename} (#{Util.format_size(file[:size])})"
          end
        end

        files.count
      end

      # Writes a bbox-filtered extract for one type to a local file and
      # returns its path (nil when the area has no data). The extract is
      # named by theme/type/release/area, so later runs reuse it as a cache.
      def extract_bbox(bbox, format: "parquet", output_path: nil)
        target_type = type or raise ArgumentError, "extract_bbox requires a type"
        path = output_path || extract_path(bbox, format: format)
        FileUtils.mkdir_p(File.dirname(path))

        sql, params = self.class.bbox_query(theme: theme, type: target_type, release: release, bbox: bbox)
        QueryEngine.instance.copy_to(sql, params: params, output_path: path,
                                     format: EXTRACT_FORMATS.fetch(format.to_s.downcase, "parquet"))

        if File.exist?(path) && File.size(path).positive?
          path
        else
          FileUtils.rm_f(path)
          nil
        end
      end

      # Extracts every type in the theme for a bbox. Returns the paths written.
      def extract_bbox_all_types(bbox, format: "parquet")
        types = type ? [type] : TYPES.fetch(theme)
        types.filter_map do |t|
          log "Querying #{theme}/#{t} (#{release})..."
          path = self.class.new(theme: theme, type: t, release: release, output_dir: output_dir)
                     .extract_bbox(bbox, format: format)
          log(path ? "  Saved #{File.basename(path)} (#{Util.format_size(File.size(path))})" : "  No data found")
          path
        end
      end

      def extract_nearby(lat:, lng:, radius_meters:, format: "parquet")
        extract_bbox_all_types(BoundingBox.around(lat: lat, lng: lng, radius_meters: radius_meters), format: format)
      end

      # The cache path for a bbox extract of this theme/type/release.
      def extract_path(bbox, format: "parquet")
        ext = EXTRACT_FORMATS.fetch(format.to_s.downcase, "parquet")
        File.join(output_dir, "#{theme}_#{type}_#{release}_#{bbox.slug}.#{ext}")
      end

      # An existing extract for this theme/type/release/area, or nil. Matches
      # exactly — never falls back to "some other file for the theme".
      def cached_extract(bbox, format: "parquet")
        path = extract_path(bbox, format: format)
        File.exist?(path) && File.size(path).positive? ? path : nil
      end

      def list_files
        prefix = "release/#{release}/theme=#{theme}#{type ? "/type=#{type}" : ""}"
        Storage.list(prefix: prefix)[:objects].select { |o| o[:key].end_with?(".parquet") }
      end

      def self.list_types(theme:, release: nil)
        release = Releases.validate!(release || Releases.current)
        listing = Storage.list(prefix: "release/#{release}/theme=#{theme}/", delimiter: "/")
        listing[:prefixes].filter_map { |p| p[/type=([^\/]+)/, 1] }.sort
      end

      def self.list_themes(release: nil)
        release = Releases.validate!(release || Releases.current)
        listing = Storage.list(prefix: "release/#{release}/", delimiter: "/")
        listing[:prefixes].filter_map { |p| p[/theme=([^\/]+)/, 1] }.sort
      end

      # Searches division areas by name. Returns hashes with :id, :name,
      # :subtype, :country, :region, :bbox (BoundingBox) and :area_km2,
      # largest areas first.
      def self.search_divisions(query:, release: nil, limit: 20)
        release = Releases.validate!(release || Releases.current)
        source = source_glob(theme: "divisions", type: "division_area", release: release)
        placeholders = DIVISION_SUBTYPES.map { "?" }.join(", ")

        sql = <<~SQL
          SELECT id, names.primary AS name, subtype, country, region,
                 bbox.xmin AS xmin, bbox.xmax AS xmax, bbox.ymin AS ymin, bbox.ymax AS ymax
          FROM read_parquet('#{source}', hive_partitioning=1)
          WHERE names.primary ILIKE ?
            AND subtype IN (#{placeholders})
            AND bbox.xmax > bbox.xmin AND bbox.ymax > bbox.ymin
          ORDER BY (bbox.xmax - bbox.xmin) * (bbox.ymax - bbox.ymin) DESC
          LIMIT #{Integer(limit)}
        SQL

        rows = QueryEngine.instance.query(sql, ["%#{query}%"] + DIVISION_SUBTYPES)
        rows.map do |row|
          bbox = BoundingBox.new(
            lat1: row["ymin"], lng1: row["xmin"],
            lat2: row["ymax"], lng2: row["xmax"],
            display_name: row["name"]
          )
          {
            id: row["id"],
            name: row["name"],
            subtype: row["subtype"],
            country: row["country"],
            region: row["region"],
            bbox: bbox,
            area_km2: Util.bbox_area_km2(bbox)
          }
        end
      end

      # Builds the bbox-filtered SELECT for one theme/type. Intersection
      # semantics: any feature whose bbox overlaps the query box is included,
      # matching what "give me this area" means (the old strict-containment
      # filter silently dropped everything touching the boundary).
      def self.bbox_query(theme:, type:, release:, bbox:)
        raise ArgumentError, "unknown theme: #{theme}" unless THEMES.include?(theme)
        raise ArgumentError, "unknown type #{type} for #{theme}" unless TYPES[theme].include?(type)

        source = source_glob(theme: theme, type: type, release: Releases.validate!(release))
        sql = <<~SQL
          SELECT *
          FROM read_parquet('#{source}', hive_partitioning=1)
          WHERE bbox.xmin <= ? AND bbox.xmax >= ?
            AND bbox.ymin <= ? AND bbox.ymax >= ?
        SQL
        [sql, [bbox.max_lng, bbox.min_lng, bbox.max_lat, bbox.min_lat]]
      end

      def self.source_glob(theme:, type:, release:)
        "#{OvertureMaps.configuration.s3_uri.chomp("/")}/release/#{release}/theme=#{theme}/type=#{type}/*.parquet"
      end

      private

      def log(message)
        logger = OvertureMaps.configuration.logger
        logger ? logger.info(message) : puts(message)
      end
    end
  end
end
