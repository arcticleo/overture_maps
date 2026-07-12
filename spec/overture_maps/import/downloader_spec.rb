# frozen_string_literal: true

RSpec.describe OvertureMaps::Import::Downloader do
  before do
    OvertureMaps.configure { |c| c.release = "2026-06-17.0" }
  end

  let(:bbox) do
    OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2, display_name: "Seattle")
  end

  describe "#initialize" do
    it "rejects unknown themes and types" do
      expect { described_class.new(theme: "nope") }.to raise_error(ArgumentError, /unknown theme/)
      expect { described_class.new(theme: "places", type: "building") }.to raise_error(ArgumentError, /unknown type/)
    end
  end

  describe ".bbox_query" do
    it "pins the release path and uses intersection semantics with bound params" do
      sql, params = described_class.bbox_query(
        theme: "places", type: "place", release: "2026-06-17.0", bbox: bbox
      )

      expect(sql).to include("release/2026-06-17.0/theme=places/type=place/*.parquet")
      expect(sql).not_to include("release/**")
      expect(sql).to include("bbox.xmin <= ? AND bbox.xmax >= ?")
      expect(params).to eq([-122.2, -122.4, 47.7, 47.5])
    end

    it "validates the release format" do
      expect {
        described_class.bbox_query(theme: "places", type: "place", release: "evil'--", bbox: bbox)
      }.to raise_error(OvertureMaps::Releases::Error)
    end
  end

  describe "#extract_path" do
    it "names extracts by theme, type, release, and area" do
      downloader = described_class.new(theme: "places", type: "place", output_dir: "tmp/cache")

      expect(downloader.extract_path(bbox))
        .to eq("tmp/cache/places_place_2026-06-17.0_seattle.parquet")
    end
  end

  describe "#cached_extract" do
    it "matches only the exact release + area, never 'most recent file'" do
      Dir.mktmpdir do |dir|
        downloader = described_class.new(theme: "places", type: "place", output_dir: dir)
        File.write(File.join(dir, "places_place_2026-06-17.0_portland.parquet"), "data")

        expect(downloader.cached_extract(bbox)).to be_nil

        exact = File.join(dir, "places_place_2026-06-17.0_seattle.parquet")
        File.write(exact, "data")
        expect(downloader.cached_extract(bbox)).to eq(exact)
      end
    end
  end

  describe ".search_divisions" do
    it "queries division areas with a bound ILIKE and maps results" do
      engine = instance_double(OvertureMaps::QueryEngine)
      allow(OvertureMaps::QueryEngine).to receive(:instance).and_return(engine)

      expect(engine).to receive(:query) do |sql, params|
        expect(sql).to include("theme=divisions/type=division_area")
        expect(sql).to include("names.primary ILIKE ?")
        expect(params.first).to eq("%O'Fallon%")
        [{
          "id" => "d1", "name" => "O'Fallon", "subtype" => "locality",
          "country" => "US", "region" => "US-MO",
          "xmin" => -90.75, "xmax" => -90.65, "ymin" => 38.75, "ymax" => 38.85
        }]
      end

      results = described_class.search_divisions(query: "O'Fallon")

      expect(results.first[:name]).to eq("O'Fallon")
      expect(results.first[:bbox]).to be_a(OvertureMaps::BoundingBox)
      expect(results.first[:area_km2]).to be > 0
    end
  end

  describe "#list_files" do
    it "lists only parquet objects under the release prefix" do
      stub_request(:get, %r{\?list-type=2&prefix=release/2026-06-17\.0/theme=places/type=place})
        .to_return(body: <<~XML)
          <?xml version="1.0"?>
          <ListBucketResult>
            <Contents><Key>release/2026-06-17.0/theme=places/type=place/part-0.parquet</Key><Size>123</Size></Contents>
            <Contents><Key>release/2026-06-17.0/theme=places/type=place/_SUCCESS</Key><Size>0</Size></Contents>
          </ListBucketResult>
        XML

      downloader = described_class.new(theme: "places", type: "place")
      files = downloader.list_files

      expect(files.length).to eq(1)
      expect(files.first[:key]).to end_with("part-0.parquet")
    end
  end
end
