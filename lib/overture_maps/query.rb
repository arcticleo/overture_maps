# frozen_string_literal: true

require "securerandom"
require "fileutils"
require "tmpdir"

module OvertureMaps
  # Ad-hoc queries against Overture GeoParquet — no PostGIS import needed.
  #
  #   OvertureMaps.query(theme: "places", location: "Seattle").limit(10).each { |r| ... }
  #   OvertureMaps.query(theme: "buildings", bbox: [47.5, -122.4, 47.7, -122.2]).count
  #   OvertureMaps.query(theme: "places", bbox: "47.5,-122.4,47.7,-122.2").export("places.geojson")
  #
  # Unlimited queries spool through the same cache files the import pipeline
  # uses (so a query warms the cache for a later import and vice versa);
  # limited queries use a throwaway temp file. Records are hashes with the
  # geometry parsed to an RGeo feature.
  class Query
    include Enumerable

    EXPORT_EXTENSIONS = {
      ".parquet" => "parquet",
      ".geojson" => "geojson",
      ".geojsonseq" => "geojsonseq",
      ".gpkg" => "gpkg"
    }.freeze

    attr_reader :theme, :type, :release

    def initialize(theme:, type: nil, bbox: nil, location: nil, release: nil, limit: nil)
      @theme = theme
      @type = type || infer_type(theme)
      @release = Releases.validate!(release || Releases.current)
      @location = location
      @bbox = coerce_bbox(bbox) if bbox
      raise ArgumentError, "provide bbox: or location:" unless @bbox || @location

      @limit = limit && Integer(limit)
    end

    def limit(count)
      self.class.new(theme: theme, type: type, bbox: bbox, location: @location,
                     release: release, limit: count)
    end

    # The resolved bounding box (resolves a location name on first use).
    def bbox
      @bbox ||= resolve_location
    end

    # Fast remote count via row-group pushdown — nothing is downloaded.
    def count
      sql, params = Import::Downloader.bbox_query(
        theme: theme, type: type, release: release, bbox: bbox, limit: @limit
      )
      rows = QueryEngine.instance.query("SELECT count(*) AS n FROM (#{sql})", params)
      Integer(rows.first["n"])
    end

    def each(&block)
      return enum_for(:each) unless block

      with_extract do |path|
        reader = Import::ParquetReader.new(theme: theme)
        reader.each_record(source: path) do |record|
          record["geometry"] = GeometryParser.parse(record["geometry"])
          block.call(record)
        end
      end
      self
    end

    def each_batch(size: 1000)
      return enum_for(:each_batch, size: size) unless block_given?

      batch = []
      each do |record|
        batch << record
        if batch.length >= size
          yield batch
          batch = []
        end
      end
      yield batch if batch.any?
      self
    end

    # Writes the query result straight to a file; format inferred from the
    # extension unless given explicitly.
    def export(path, format: nil)
      format ||= EXPORT_EXTENSIONS[File.extname(path).downcase] or
        raise ArgumentError, "cannot infer format from #{path}; pass format:"

      downloader.extract_bbox(bbox, format: format, output_path: path, limit: @limit) or
        raise Error, "query returned no data"
    end

    # A GeoJSON FeatureCollection hash. Materializes everything — use
    # #export for large results.
    def to_geojson
      features = map do |record|
        geometry = record["geometry"]
        {
          type: "Feature",
          geometry: geometry && RGeo::GeoJSON.encode(geometry),
          properties: record.except("geometry", "bbox")
        }
      end
      { type: "FeatureCollection", features: features }
    end

    private

    def infer_type(theme)
      types = Import::Downloader.types_for_theme(theme)
      raise ArgumentError, "unknown theme: #{theme}" if types.empty?
      return types.first if types.length == 1

      raise ArgumentError, "theme #{theme} has multiple types (#{types.join(", ")}); pass type:"
    end

    def coerce_bbox(value)
      case value
      when BoundingBox then value
      when Array
        raise ArgumentError, "bbox array must be [lat1, lng1, lat2, lng2]" unless value.length == 4

        BoundingBox.new(lat1: value[0], lng1: value[1], lat2: value[2], lng2: value[3])
      when String
        BoundingBox.parse(value) or raise ArgumentError, "unparseable bbox: #{value.inspect}"
      else
        raise ArgumentError, "bbox must be a BoundingBox, array, or string"
      end
    end

    def resolve_location
      results = DivisionSearch.search(query: @location, release: release)
      raise Error, "no divisions found matching #{@location.inspect}" if results.empty?

      results.first[:bbox]
    end

    def downloader
      Import::Downloader.new(theme: theme, type: type, release: release)
    end

    # Unlimited queries share the import pipeline's cache files; limited
    # queries spool to a temp file that is removed afterwards.
    def with_extract
      if @limit
        path = File.join(Dir.tmpdir, "overture_query_#{SecureRandom.hex(8)}.parquet")
        begin
          extracted = downloader.extract_bbox(bbox, output_path: path, limit: @limit)
          yield extracted if extracted
        ensure
          FileUtils.rm_f(path)
        end
      else
        path = downloader.cached_extract(bbox) || downloader.extract_bbox(bbox)
        yield path if path
      end
    end
  end

  def self.query(**options)
    Query.new(**options)
  end
end
