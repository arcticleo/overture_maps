# frozen_string_literal: true

RSpec.describe OvertureMaps::Changelog do
  let(:engine) { instance_double(OvertureMaps::QueryEngine) }
  let(:bbox) { OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2) }

  before do
    allow(OvertureMaps::QueryEngine).to receive(:instance).and_return(engine)
  end

  describe ".removed_ids" do
    it "queries the removed partition with bbox intersection params" do
      expect(engine).to receive(:query) do |sql, params|
        expect(sql).to include("changelog/2026-06-17.0/theme=places/type=place/change_type=removed")
        expect(sql).to include("bbox.xmin <= ? AND bbox.xmax >= ?")
        expect(params).to eq([-122.2, -122.4, 47.7, 47.5])
        [{ "id" => "gone-1" }, { "id" => "gone-2" }]
      end

      ids = described_class.removed_ids(theme: "places", type: "place",
                                        release: "2026-06-17.0", bbox: bbox)
      expect(ids).to eq(%w[gone-1 gone-2])
    end

    it "omits the WHERE clause without a bbox" do
      expect(engine).to receive(:query) do |sql, params|
        expect(sql).not_to include("WHERE")
        expect(params).to eq([])
        []
      end

      described_class.removed_ids(theme: "places", type: "place", release: "2026-06-17.0")
    end

    it "validates theme, type, and release" do
      expect {
        described_class.removed_ids(theme: "nope", type: "place", release: "2026-06-17.0")
      }.to raise_error(ArgumentError, /unknown theme/)

      expect {
        described_class.removed_ids(theme: "places", type: "place", release: "evil'--")
      }.to raise_error(OvertureMaps::Releases::Error)
    end
  end

  describe ".counts" do
    it "groups by change_type across all partitions" do
      expect(engine).to receive(:query) do |sql, _params|
        expect(sql).to include("change_type=*")
        expect(sql).to include("GROUP BY change_type")
        [{ "change_type" => "added", "n" => 10 }, { "change_type" => "removed", "n" => 2 }]
      end

      counts = described_class.counts(theme: "places", type: "place", release: "2026-06-17.0")
      expect(counts).to eq({ "added" => 10, "removed" => 2 })
    end
  end
end
