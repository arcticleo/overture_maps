# frozen_string_literal: true

require "support/engine_harness"
require "rack/test"
require "json"

RSpec.describe "Engine API" do
  include Rack::Test::Methods

  def app
    Rails.application
  end

  before do
    skip EngineHarness.skip_reason unless EngineHarness.available?
    WebMock.disable_net_connect!(allow_localhost: true)
  end

  def json_body
    JSON.parse(last_response.body)
  end

  describe "GET /overture/:resource" do
    it "lists features within a bbox as JSON with geometry" do
      get "/overture/places", bbox: "-122.35,47.60,-122.33,47.62"

      expect(last_response.status).to eq(200)
      names = json_body["data"].map { |p| p["name"] }
      expect(names).to include("Pike Street Coffee", "Seattle Art Museum")
      expect(names).not_to include("Elm Coffee Roasters")
      expect(json_body["data"].first["geometry"]["type"]).to eq("Point")
    end

    it "returns GeoJSON FeatureCollections on request" do
      get "/overture/places", bbox: "-122.35,47.60,-122.33,47.62", format: "geojson"

      expect(json_body["type"]).to eq("FeatureCollection")
      expect(json_body["features"].first["type"]).to eq("Feature")
    end

    it "filters by near, q, and category" do
      get "/overture/places", near: "47.609,-122.34,100"
      expect(json_body["data"].map { |p| p["id"] }).to eq(["p-cafe-1"])

      get "/overture/places", q: "museum"
      expect(json_body["data"].map { |p| p["id"] }).to eq(["p-museum"])

      get "/overture/places", category: "coffee_shop"
      expect(json_body["data"].length).to eq(2)
    end

    it "paginates with a keyset cursor" do
      get "/overture/places", limit: 2
      expect(json_body["data"].length).to eq(2)
      cursor = json_body["meta"]["next_cursor"]
      expect(cursor).not_to be_nil

      get "/overture/places", limit: 2, after: cursor
      expect(json_body["data"].length).to eq(1)
      expect(json_body["meta"]["next_cursor"]).to be_nil
    end

    it "rejects malformed parameters" do
      get "/overture/places", bbox: "1,2,3"
      expect(last_response.status).to eq(400)

      get "/overture/places", bbox: "-122.35,95,-122.33,96"
      expect(last_response.status).to eq(400)

      get "/overture/places", limit: "-5"
      expect(last_response.status).to eq(400)
    end

    it "404s unknown resources via route constraints" do
      get "/overture/users"
      expect(last_response.status).to eq(404)
    end
  end

  describe "GET /overture/:resource/:id" do
    it "returns a single feature" do
      get "/overture/places/p-cafe-1"

      expect(last_response.status).to eq(200)
      expect(json_body["name"]).to eq("Pike Street Coffee")
    end

    it "404s unknown ids" do
      get "/overture/places/nope"
      expect(last_response.status).to eq(404)
    end
  end

  describe "GET /overture/search" do
    it "geocodes from the local divisions table" do
      get "/overture/search", q: "Seattle"

      expect(last_response.status).to eq(200)
      result = json_body["data"].first
      expect(result["name"]).to eq("Seattle")
      expect(result["bbox"]).to eq([-122.46, 47.48, -122.22, 47.73])
    end

    it "requires q" do
      get "/overture/search"
      expect(last_response.status).to eq(422)
    end
  end

  describe "GET /overture/tiles" do
    # Slippy tile containing a lon/lat at a zoom level.
    def tile_for(lng, lat, zoom)
      n = 2**zoom
      x = ((lng + 180.0) / 360.0 * n).floor
      lat_rad = lat * Math::PI / 180.0
      y = ((1.0 - Math.log(Math.tan(lat_rad) + 1 / Math.cos(lat_rad)) / Math::PI) / 2.0 * n).floor
      [x, y]
    end

    it "serves MVT bytes for a tile containing data" do
      x, y = tile_for(-122.34, 47.609, 14)
      get "/overture/tiles/places/14/#{x}/#{y}.mvt"

      expect(last_response.status).to eq(200)
      expect(last_response.content_type).to include("application/vnd.mapbox-vector-tile")
      expect(last_response.body.bytesize).to be > 20
      expect(last_response.body).to include("places") # layer name is embedded in the tile
      expect(last_response.headers["Cache-Control"]).to include("public")
    end

    it "serves an empty tile where there is no data" do
      get "/overture/tiles/places/14/0/0.mvt"

      expect(last_response.status).to eq(200)
      expect(last_response.body.bytesize).to be <= 2
    end

    it "rejects out-of-range coordinates and unknown layers" do
      get "/overture/tiles/places/2/9/1.mvt"
      expect(last_response.status).to eq(400)

      get "/overture/tiles/users/2/1/1.mvt"
      expect(last_response.status).to eq(404)
    end
  end

  describe "auth hook" do
    it "lets config.api_auth reject requests" do
      OvertureMaps.configure do |c|
        c.api_auth = ->(controller) { controller.head :unauthorized }
      end

      get "/overture/places"
      expect(last_response.status).to eq(401)
    end

    it "passes requests through when the hook approves" do
      OvertureMaps.configure do |c|
        c.api_auth = ->(controller) {} # rubocop:disable Lint/EmptyBlock
      end

      get "/overture/places"
      expect(last_response.status).to eq(200)
    end
  end
end
