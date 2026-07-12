# frozen_string_literal: true

module OvertureMaps
  # Mapbox Vector Tiles straight from PostGIS:
  #
  #   GET /overture/tiles/places/14/2620/5723.mvt
  #
  # MapLibre/Leaflet can render imported data with no separate tile server:
  #   map.addSource("places", { type: "vector",
  #     tiles: ["https://app.example.com/overture/tiles/places/{z}/{x}/{y}.mvt"] })
  class TilesController < ApplicationController
    LAYERS = FeaturesController::RESOURCES

    PROPERTY_COLUMNS = {
      "places" => %w[name primary_category confidence country],
      "buildings" => %w[name subtype building_class height num_floors],
      "addresses" => %w[number street locality postcode],
      "divisions" => %w[name subtype country],
      "segments" => %w[name subtype segment_class],
      "connectors" => %w[],
      "base_features" => %w[name feature_type subtype feature_class]
    }.freeze

    MAX_ZOOM = 22
    TILE_EXTENT = 4096
    TILE_BUFFER = 64
    # Half a buffer's worth of envelope margin so buffered geometries at the
    # tile edge are included.
    ENVELOPE_MARGIN = TILE_BUFFER.to_f / TILE_EXTENT

    def show
      layer = params[:layer]
      model = LAYERS[layer]
      return render json: { error: "unknown layer" }, status: :not_found unless model

      z, x, y = tile_coordinates
      tile = build_tile(model, layer, z, x, y)

      expires_in 1.hour, public: true
      send_data tile, type: "application/vnd.mapbox-vector-tile", disposition: "inline"
    end

    private

    def tile_coordinates
      z = Integer(params[:z])
      x = Integer(params[:x])
      y = Integer(params[:y])
      max_index = (2**z) - 1
      unless z.between?(0, MAX_ZOOM) && x.between?(0, max_index) && y.between?(0, max_index)
        raise ArgumentError, "tile coordinates out of range"
      end

      [z, x, y]
    end

    def build_tile(model, layer, z, x, y)
      connection = model.connection
      columns = (PROPERTY_COLUMNS.fetch(layer, []) & model.column_names)
                .map { |c| ", #{connection.quote_column_name(c)}" }.join

      sql = ActiveRecord::Base.sanitize_sql(
        [<<~SQL, { z: z, x: x, y: y, margin: ENVELOPE_MARGIN, limit: OvertureMaps.configuration.tile_feature_limit }]
          WITH mvtgeom AS (
            SELECT ST_AsMVTGeom(
                     ST_Transform(geometry::geometry, 3857),
                     ST_TileEnvelope(:z, :x, :y),
                     #{TILE_EXTENT}, #{TILE_BUFFER}, true
                   ) AS geom,
                   id#{columns}
            FROM #{model.quoted_table_name}
            WHERE geometry && ST_Transform(ST_TileEnvelope(:z, :x, :y, margin => :margin), 4326)::geography
            LIMIT :limit
          )
          SELECT ST_AsMVT(mvtgeom.*, #{connection.quote(layer)}, #{TILE_EXTENT}, 'geom')
          FROM mvtgeom WHERE geom IS NOT NULL
        SQL
      )

      value = connection.select_value(sql) || ""
      # Raw query results may arrive as hex-escaped bytea.
      value.start_with?("\\x") ? [value.delete_prefix("\\x")].pack("H*") : value
    end
  end
end
