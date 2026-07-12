# frozen_string_literal: true

module OvertureMaps
  class ApplicationController < ActionController::API
    before_action :authenticate!

    rescue_from ArgumentError do |error|
      render json: { error: error.message }, status: :bad_request
    end

    private

    # Hosts wrap the API in their own auth:
    #   OvertureMaps.configure do |c|
    #     c.api_auth = ->(controller) {
    #       controller.head :unauthorized unless controller.request.headers["X-Api-Key"] == ...
    #     }
    #   end
    # Rendering or heading inside the hook halts the request.
    def authenticate!
      OvertureMaps.configuration.api_auth&.call(self)
    end

    def geojson?
      params[:format] == "geojson" || request.headers["Accept"].to_s.include?("geo+json")
    end
  end
end
