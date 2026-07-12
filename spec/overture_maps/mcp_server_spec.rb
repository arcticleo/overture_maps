# frozen_string_literal: true

mcp_available =
  begin
    require "mcp"
    true
  rescue LoadError
    false
  end
require "overture_maps/mcp_server" if mcp_available

RSpec.describe "OvertureMaps::MCPServer", skip: (mcp_available ? false : "mcp gem not installed") do
  let(:bbox) { OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2) }

  def payload(response)
    JSON.parse(response.content.first[:text])
  end

  it "registers the read-only tool set" do
    names = OvertureMaps::MCPServer::TOOLS.map(&:tool_name)

    expect(names).to match_array(
      %w[geocode query_features count_features export_geojson gers_lookup list_releases]
    )
  end

  describe "geocode" do
    it "returns divisions with GeoJSON-ordered bboxes" do
      allow(OvertureMaps::DivisionSearch).to receive(:search).with(query: "Seattle").and_return(
        [{ id: "d1", name: "Seattle", subtype: "locality", country: "US", region: "US-WA",
           bbox: bbox, area_km2: 350.0 }]
      )

      result = payload(OvertureMaps::MCPServer::GeocodeTool.call(query: "Seattle"))

      expect(result.first["name"]).to eq("Seattle")
      expect(result.first["bbox"]).to eq([-122.4, 47.5, -122.2, 47.7])
    end

    it "reports errors as tool errors, not exceptions" do
      allow(OvertureMaps::DivisionSearch).to receive(:search).and_raise(OvertureMaps::Error, "offline")

      response = OvertureMaps::MCPServer::GeocodeTool.call(query: "x")

      expect(payload(response)["error"]).to eq("offline")
    end
  end

  describe "count_features" do
    it "counts within explicit corners" do
      query = instance_double(OvertureMaps::Query, count: 42)
      expect(OvertureMaps::Query).to receive(:new) do |args|
        expect(args[:theme]).to eq("places")
        expect(args[:bbox].min_lng).to eq(-122.4)
        query
      end

      result = payload(OvertureMaps::MCPServer::CountFeaturesTool.call(
                         theme: "places", west: -122.4, south: 47.5, east: -122.2, north: 47.7
                       ))

      expect(result["count"]).to eq(42)
    end

    it "requires an area" do
      response = OvertureMaps::MCPServer::CountFeaturesTool.call(theme: "places")

      expect(payload(response)["error"]).to match(/west\/south\/east\/north or a location/)
    end
  end

  describe "query_features" do
    it "builds named GeoJSON features and applies category filters" do
      records = [
        { "id" => "1", "geometry" => OvertureMaps::GeometryParser.parse("POINT (1 2)"),
          "names" => { "primary" => "Cafe A" }, "categories" => { "primary" => "cafe" } },
        { "id" => "2", "geometry" => OvertureMaps::GeometryParser.parse("POINT (3 4)"),
          "names" => { "primary" => "Museum" }, "categories" => { "primary" => "art_museum" } }
      ]
      query = double("query")
      allow(query).to receive(:each) { |&block| records.each(&block) }
      allow(OvertureMaps::Query).to receive(:new).and_return(query)

      result = payload(OvertureMaps::MCPServer::QueryFeaturesTool.call(
                         theme: "places", category: "cafe",
                         west: -122.4, south: 47.5, east: -122.2, north: 47.7
                       ))

      expect(result["features"].length).to eq(1)
      feature = result["features"].first
      expect(feature["properties"]["name"]).to eq("Cafe A")
      expect(feature["geometry"]["type"]).to eq("Point")
    end
  end
end
