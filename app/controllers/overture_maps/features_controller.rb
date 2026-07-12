# frozen_string_literal: true

module OvertureMaps
  # Read-only access to imported Overture data.
  #
  #   GET /overture/places?bbox=-122.4,47.5,-122.2,47.7&category=cafe&limit=50
  #   GET /overture/places?near=47.6,-122.3,1000&format=geojson
  #   GET /overture/buildings/<gers-id>
  #
  # Collections paginate by keyset: pass meta.next_cursor back as ?after=.
  # ?format=geojson (or Accept: application/geo+json) returns GeoJSON.
  class FeaturesController < ApplicationController
    RESOURCES = {
      "places" => Models::Place,
      "buildings" => Models::Building,
      "addresses" => Models::Address,
      "divisions" => Models::Division,
      "segments" => Models::Segment,
      "connectors" => Models::Connector,
      "base_features" => Models::BaseFeature
    }.freeze

    MAX_RADIUS_METERS = 100_000

    def index
      scope = apply_filters(model.all)
      scope = scope.where("id > ?", params[:after].to_s) if params[:after].present?
      records = scope.order(:id).limit(page_size).to_a

      if geojson?
        render json: {
          type: "FeatureCollection",
          features: records.map(&:to_geojson)
        }
      else
        render json: {
          data: records.map { |record| serialize(record) },
          meta: {
            count: records.length,
            next_cursor: records.length == page_size ? records.last&.id : nil
          }
        }
      end
    end

    def show
      record = model.find_by(id: params[:id])
      return render json: { error: "not found" }, status: :not_found unless record

      render json: geojson? ? record.to_geojson : serialize(record)
    end

    private

    def model
      RESOURCES.fetch(params[:resource])
    end

    def apply_filters(scope)
      scope = filter_bbox(scope)
      scope = filter_near(scope)
      scope = filter_name(scope)
      scope = filter_columns(scope)
      scope = scope.by_category(params[:category].to_s.split(",")) if params[:category].present? && model == Models::Place
      scope
    end

    def filter_bbox(scope)
      return scope unless params[:bbox].present?

      # GeoJSON bbox order: west,south,east,north
      values = params[:bbox].split(",").map { |v| Float(v) }
      raise ArgumentError, "bbox must be west,south,east,north" unless values.length == 4

      west, south, east, north = values
      unless south.between?(-90, 90) && north.between?(-90, 90) &&
             west.between?(-180, 180) && east.between?(-180, 180)
        raise ArgumentError, "bbox out of range"
      end

      scope.within_bounds(south, west, north, east)
    end

    def filter_near(scope)
      return scope unless params[:near].present?

      values = params[:near].split(",").map { |v| Float(v) }
      raise ArgumentError, "near must be lat,lng[,radius_meters]" unless values.length.between?(2, 3)

      lat, lng = values
      radius = (values[2] || 1000).clamp(1, MAX_RADIUS_METERS)
      raise ArgumentError, "near out of range" unless lat.between?(-90, 90) && lng.between?(-180, 180)

      scope.near(lat, lng, radius)
    end

    def filter_name(scope)
      return scope unless params[:q].present? && model.column_names.include?("name")

      scope.where("name ILIKE ?", "%#{model.sanitize_sql_like(params[:q])}%")
    end

    def filter_columns(scope)
      %w[country subtype operating_status].each do |column|
        next unless params[column].present? && model.column_names.include?(column)

        scope = scope.where(column => params[column])
      end
      scope
    end

    def page_size
      requested = params[:limit].present? ? Integer(params[:limit]) : OvertureMaps.configuration.api_default_limit
      raise ArgumentError, "limit must be positive" unless requested.positive?

      [requested, OvertureMaps.configuration.api_max_limit].min
    end

    def serialize(record)
      attributes = record.attributes.except("geometry")
      attributes["geometry"] = record.geometry && RGeo::GeoJSON.encode(record.geometry)
      attributes
    end
  end
end
