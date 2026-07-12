# frozen_string_literal: true

RSpec.describe OvertureMaps::Query do
  before do
    OvertureMaps.configure { |c| c.release = "2026-06-17.0" }
  end

  let(:bbox_array) { [47.5, -122.4, 47.7, -122.2] }

  describe "construction" do
    it "infers the type for single-type themes" do
      query = described_class.new(theme: "places", bbox: bbox_array)

      expect(query.type).to eq("place")
    end

    it "requires an explicit type for multi-type themes" do
      expect {
        described_class.new(theme: "transportation", bbox: bbox_array)
      }.to raise_error(ArgumentError, /pass type:/)

      query = described_class.new(theme: "transportation", type: "segment", bbox: bbox_array)
      expect(query.type).to eq("segment")
    end

    it "coerces bbox from array, string, and BoundingBox" do
      from_array = described_class.new(theme: "places", bbox: bbox_array)
      from_string = described_class.new(theme: "places", bbox: "47.5,-122.4,47.7,-122.2")
      box = OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2)
      from_box = described_class.new(theme: "places", bbox: box)

      expect(from_array.bbox.to_s).to eq(from_string.bbox.to_s)
      expect(from_box.bbox).to equal(box)
    end

    it "rejects malformed bboxes and missing area" do
      expect { described_class.new(theme: "places", bbox: [1, 2]) }.to raise_error(ArgumentError)
      expect { described_class.new(theme: "places", bbox: "nonsense") }.to raise_error(ArgumentError)
      expect { described_class.new(theme: "places") }.to raise_error(ArgumentError, /bbox: or location:/)
    end

    it "resolves location names through DivisionSearch lazily" do
      box = OvertureMaps::BoundingBox.new(lat1: 47, lng1: -123, lat2: 48, lng2: -122)
      expect(OvertureMaps::DivisionSearch).to receive(:search)
        .with(query: "Seattle", release: "2026-06-17.0")
        .and_return([{ name: "Seattle", bbox: box }])

      query = described_class.new(theme: "places", location: "Seattle")

      expect(query.bbox).to equal(box)
    end
  end

  describe "#limit" do
    it "returns a new query without mutating the original" do
      query = described_class.new(theme: "places", bbox: bbox_array)
      limited = query.limit(10)

      expect(limited).not_to equal(query)
      expect(limited.instance_variable_get(:@limit)).to eq(10)
      expect(query.instance_variable_get(:@limit)).to be_nil
    end
  end

  describe "#count" do
    it "wraps the bbox query in a remote count" do
      engine = instance_double(OvertureMaps::QueryEngine)
      allow(OvertureMaps::QueryEngine).to receive(:instance).and_return(engine)

      expect(engine).to receive(:query) do |sql, params|
        expect(sql).to start_with("SELECT count(*) AS n FROM (")
        expect(sql).to include("release/2026-06-17.0/theme=places/type=place")
        expect(params.length).to eq(4)
        [{ "n" => 42 }]
      end

      query = described_class.new(theme: "places", bbox: bbox_array)
      expect(query.count).to eq(42)
    end
  end

  describe "#export" do
    it "infers the format from the extension" do
      query = described_class.new(theme: "places", bbox: bbox_array)
      downloader = instance_double(OvertureMaps::Import::Downloader)
      allow(query).to receive(:downloader).and_return(downloader)

      expect(downloader).to receive(:extract_bbox)
        .with(query.bbox, format: "geojson", output_path: "out.geojson", limit: nil)
        .and_return("out.geojson")

      query.export("out.geojson")
    end

    it "raises for unknown extensions without an explicit format" do
      query = described_class.new(theme: "places", bbox: bbox_array)

      expect { query.export("out.xyz") }.to raise_error(ArgumentError, /cannot infer format/)
    end
  end

  describe "streaming", :duckdb do
    before do
      engine = OvertureMaps::QueryEngine.instance
      skip "no DuckDB available" unless engine.native? || OvertureMaps::QueryEngine.cli_available?
    end

    def build_fixture(dir)
      WebMock.allow_net_connect!
      path = File.join(dir, "fixture.parquet")
      OvertureMaps::QueryEngine.instance.copy_to(
        "SELECT * FROM (VALUES ('a1', 'POINT (1 2)', 'Alpha'), ('b2', 'POINT (3 4)', 'Beta')) t(id, geometry, name)",
        params: [], output_path: path
      )
      path
    ensure
      WebMock.disable_net_connect!
    end

    it "streams records with parsed geometry and builds GeoJSON" do
      Dir.mktmpdir do |dir|
        fixture = build_fixture(dir)
        query = described_class.new(theme: "places", bbox: bbox_array)
        downloader = instance_double(OvertureMaps::Import::Downloader, cached_extract: fixture)
        allow(query).to receive(:downloader).and_return(downloader)

        records = query.to_a
        expect(records.length).to eq(2)
        expect(records.first["geometry"]).to be_a(RGeo::Feature::Instance)
        expect(records.first["geometry"].x).to eq(1.0)

        geojson = query.to_geojson
        expect(geojson[:type]).to eq("FeatureCollection")
        expect(geojson[:features].length).to eq(2)
        expect(geojson[:features].first[:properties]["name"]).to eq("Alpha")
        expect(geojson[:features].first[:properties]).not_to have_key("geometry")
      end
    end

    it "batches records" do
      Dir.mktmpdir do |dir|
        fixture = build_fixture(dir)
        query = described_class.new(theme: "places", bbox: bbox_array)
        allow(query).to receive(:downloader)
          .and_return(instance_double(OvertureMaps::Import::Downloader, cached_extract: fixture))

        batches = query.each_batch(size: 1).to_a
        expect(batches.length).to eq(2)
        expect(batches.first.length).to eq(1)
      end
    end
  end

  describe "OvertureMaps.query" do
    it "builds a Query" do
      query = OvertureMaps.query(theme: "places", bbox: bbox_array)

      expect(query).to be_a(described_class)
    end
  end
end
