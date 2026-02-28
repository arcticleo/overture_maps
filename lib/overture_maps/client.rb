module OvertureMaps
  class Client
    BASE_URL = "https://api.overturemapsapi.com".freeze

    def initialize(configuration = OvertureMaps.configuration)
      @configuration = configuration
    end

    # Query buildings within a geographic area
    #
    # @param lat [Float] Latitude
    # @param lng [Float] Longitude
    # @param radius [Integer] Search radius in meters (max 50000)
    # @param limit [Integer] Maximum results (max 25000)
    # @param format [String] Response format: "json", "csv", or "geojson"
    # @param includes [String] Additional fields to include
    # @return [Hash, Array] Parsed API response
    #
    # @example
    #   client.buildings(lat: 40.7128, lng: -74.006, radius: 1000)
    def buildings(lat: nil, lng: nil, radius: nil, limit: nil, format: "json", includes: nil)
      params = build_params({
        lat: lat,
        lng: lng,
        radius: radius,
        limit: limit,
        format: format,
        includes: includes
      })

      get("/buildings", params)
    end

    # Query places within a geographic area or by other filters
    #
    # @param lat [Float] Latitude
    # @param lng [Float] Longitude
    # @param radius [Integer] Search radius in meters (max 50000)
    # @param categories [String, Array] Category filter (e.g., "cafes", "restaurants")
    # @param brand_name [String] Brand name filter
    # @param country [String] ISO 3166-1 alpha-2 country code
    # @param limit [Integer] Maximum results (max 25000)
    # @param format [String] Response format: "json", "csv", or "geojson"
    # @return [Hash, Array] Parsed API response
    #
    # @example
    #   client.places(lat: -33.8910, lng: 151.2769, radius: 2000, categories: "cafes")
    #   client.places(country: "JP", categories: "cafes", limit: 10)
    def places(lat: nil, lng: nil, radius: nil, categories: nil, brand_name: nil, country: nil, limit: nil, format: "json")
      params = build_params({
        lat: lat,
        lng: lng,
        radius: radius,
        categories: (Array(categories).join(",") if categories),
        brand_name: brand_name,
        country: country,
        limit: limit,
        format: format
      })

      get("/places", params)
    end

    # Query places that have associated buildings
    #
    # @param lat [Float] Latitude
    # @param lng [Float] Longitude
    # @param radius [Integer] Search radius in meters
    # @param categories [String, Array] Category filter
    # @param brand_name [String] Brand name filter
    # @param country [String] ISO 3166-1 alpha-2 country code
    # @param limit [Integer] Maximum results
    # @param format [String] Response format
    # @return [Hash, Array] Parsed API response
    def places_with_buildings(lat: nil, lng: nil, radius: nil, categories: nil, brand_name: nil, country: nil, limit: nil, format: "json")
      params = build_params({
        lat: lat,
        lng: lng,
        radius: radius,
        categories: (Array(categories).join(",") if categories),
        brand_name: brand_name,
        country: country,
        limit: limit,
        format: format
      })

      get("/places/buildings", params)
    end

    # Get list of brands, optionally filtered by country
    #
    # @param country [String] ISO 3166-1 alpha-2 country code
    # @param limit [Integer] Maximum results
    # @return [Hash, Array] Parsed API response
    #
    # @example
    #   client.brands(country: "US")
    def brands(country: nil, limit: nil)
      params = build_params({
        country: country,
        limit: limit
      })

      get("/places/brands", params)
    end

    # Get list of countries with place counts
    #
    # @return [Hash, Array] Parsed API response
    def countries
      get("/places/countries", {})
    end

    # Get list of available categories
    #
    # @return [Hash, Array] Parsed API response
    def categories
      get("/places/categories", {})
    end

    private

    def connection
      @connection ||= Faraday.new(url: BASE_URL) do |f|
        f.request :json
        f.response :json
        f.adapter @configuration.adapter
        f.options.timeout = @configuration.timeout
        f.headers["x-api-key"] = @configuration.api_key
      end
    end

    def get(path, params)
      response = connection.get(path, params.reject { |_k, v| v.nil? })

      raise APIError, "API request failed: #{response.status}" unless response.success?

      response.body
    rescue Faraday::Error => e
      raise APIError, "HTTP error: #{e.message}"
    end

    def build_params(hash)
      hash.reject { |_k, v| v.nil? }
    end
  end
end
