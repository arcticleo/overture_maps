# frozen_string_literal: true

module OvertureMaps
  # Division geocoding: GET /overture/search?q=Seattle
  # Resolves from the local overture_divisions table when populated,
  # falling back to Overture's bucket.
  class SearchController < ApplicationController
    def index
      query = params[:q].to_s.strip
      return render json: { error: "q is required" }, status: :unprocessable_entity if query.empty?

      results = DivisionSearch.search(query: query)
      render json: {
        data: results.map do |result|
          {
            id: result[:id],
            name: result[:name],
            subtype: result[:subtype],
            country: result[:country],
            region: result[:region],
            area_km2: result[:area_km2],
            bbox: [result[:bbox].min_lng, result[:bbox].min_lat,
                   result[:bbox].max_lng, result[:bbox].max_lat]
          }
        end
      }
    rescue OvertureMaps::Error => e
      render json: { error: e.message }, status: :bad_gateway
    end
  end
end
