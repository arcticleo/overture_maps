# frozen_string_literal: true

RSpec.describe OvertureMaps::Client do
  let(:configuration) { OvertureMaps::Configuration.new }
  let(:client) { described_class.new(configuration) }

  before do
    configuration.api_key = "test-api-key"
  end

  describe "#initialize" do
    it "accepts a configuration object" do
      expect(client).to be_a described_class
    end
  end

  describe "#buildings" do
    it "makes a GET request to /buildings endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/buildings/)
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.buildings(lat: 40.7128, lng: -74.006, radius: 1000)

      expect(a_request(:get, /api.overturemapsapi.com\/buildings/)).to have_been_made
    end

    it "includes API key in request headers" do
      stub_request(:get, /api.overturemapsapi.com\/buildings/)
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.buildings(lat: 40.7128, lng: -74.006, radius: 1000)

      expect(a_request(:get, /api.overturemapsapi.com\/buildings/)
        .with(headers: { "x-api-key" => "test-api-key" })).to have_been_made
    end

    it "builds correct query parameters" do
      stub_request(:get, /api.overturemapsapi.com\/buildings/)
        .with(query: hash_including(
          "lat" => "40.7128",
          "lng" => "-74.006",
          "radius" => "1000",
          "limit" => "100",
          "format" => "json"
        ))
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.buildings(lat: 40.7128, lng: -74.006, radius: 1000, limit: 100)
    end
  end

  describe "#places" do
    it "makes a GET request to /places endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/places/)
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.places(lat: -33.8910, lng: 151.2769, radius: 2000, categories: "cafes")

      expect(a_request(:get, /api.overturemapsapi.com\/places/)).to have_been_made
    end

    it "accepts array of categories" do
      stub_request(:get, /api.overturemapsapi.com\/places/)
        .with(query: hash_including("categories" => "cafes,restaurants"))
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.places(lat: -33.8910, lng: 151.2769, radius: 2000, categories: %w[cafes restaurants])
    end

    it "supports country filter without lat/lng" do
      stub_request(:get, /api.overturemapsapi.com\/places/)
        .with(query: hash_including("country" => "JP", "categories" => "cafes", "limit" => "10"))
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.places(country: "JP", categories: "cafes", limit: 10)
    end
  end

  describe "#places_with_buildings" do
    it "makes a GET request to /places/buildings endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/places\/buildings/)
        .to_return(
          status: 200,
          body: { features: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.places_with_buildings(lat: 40.7128, lng: -74.006, radius: 1000)

      expect(a_request(:get, /api.overturemapsapi.com\/places\/buildings/)).to have_been_made
    end
  end

  describe "#brands" do
    it "makes a GET request to /places/brands endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/places\/brands/)
        .to_return(
          status: 200,
          body: { brands: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.brands(country: "US")

      expect(a_request(:get, /api.overturemapsapi.com\/places\/brands/)).to have_been_made
    end
  end

  describe "#countries" do
    it "makes a GET request to /places/countries endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/places\/countries/)
        .to_return(
          status: 200,
          body: { countries: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.countries

      expect(a_request(:get, /api.overturemapsapi.com\/places\/countries/)).to have_been_made
    end
  end

  describe "#categories" do
    it "makes a GET request to /places/categories endpoint" do
      stub_request(:get, /api.overturemapsapi.com\/places\/categories/)
        .to_return(
          status: 200,
          body: { categories: [] }.to_json,
          headers: { "Content-Type" => "application/json" }
        )

      client.categories

      expect(a_request(:get, /api.overturemapsapi.com\/places\/categories/)).to have_been_made
    end
  end

  describe "error handling" do
    it "raises APIError on non-success response" do
      stub_request(:get, /api.overturemapsapi.com\/places/)
        .to_return(status: 401, body: '{"error":"Unauthorized"}')

      expect { client.places(lat: 40.7128, lng: -74.006) }
        .to raise_error(OvertureMaps::APIError, /API request failed: 401/)
    end

    it "raises APIError on connection error" do
      stub_request(:get, /api.overturemapsapi.com\/places/)
        .to_raise(Faraday::TimeoutError)

      expect { client.places(lat: 40.7128, lng: -74.006) }
        .to raise_error(OvertureMaps::APIError, /HTTP error:/)
    end
  end
end
