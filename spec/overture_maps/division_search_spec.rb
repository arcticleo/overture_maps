# frozen_string_literal: true

RSpec.describe OvertureMaps::DivisionSearch do
  let(:remote_results) do
    [{ id: "r1", name: "Seattle", subtype: "locality", country: "US", region: "US-WA",
       bbox: OvertureMaps::BoundingBox.new(lat1: 47, lng1: -123, lat2: 48, lng2: -122),
       area_km2: 100.0 }]
  end

  describe ".search" do
    it "falls back to the bucket when the local table is unavailable" do
      # No database connection in specs, so table_exists? raises → remote.
      expect(OvertureMaps::Import::Downloader)
        .to receive(:search_divisions).with(query: "Seattle", release: nil, limit: 20)
        .and_return(remote_results)

      expect(described_class.search(query: "Seattle")).to eq(remote_results)
    end

    it "falls back to the bucket when the local table has no match" do
      allow(OvertureMaps::Models::Division).to receive(:table_exists?).and_return(true)
      relation = double("relation")
      allow(OvertureMaps::Models::Division).to receive(:search_by_name).and_return(relation)
      allow(relation).to receive_message_chain(:largest_first, :limit).and_return([])

      expect(OvertureMaps::Import::Downloader).to receive(:search_divisions).and_return(remote_results)

      expect(described_class.search(query: "Seattle")).to eq(remote_results)
    end

    it "serves results from the local table when populated" do
      # Attribute readers come from DB columns, so a verifying double can't
      # see them without a connection.
      division = double(
        "OvertureDivision",
        id: "d1", name: "Seattle", subtype: "locality", country: "US", region: "US-WA",
        to_bounding_box: OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2)
      )
      allow(OvertureMaps::Models::Division).to receive(:table_exists?).and_return(true)
      relation = double("relation")
      allow(OvertureMaps::Models::Division).to receive(:search_by_name).with("Seattle").and_return(relation)
      allow(relation).to receive_message_chain(:largest_first, :limit).and_return([division])

      expect(OvertureMaps::Import::Downloader).not_to receive(:search_divisions)

      results = described_class.search(query: "Seattle")

      expect(results.first[:id]).to eq("d1")
      expect(results.first[:bbox]).to be_a(OvertureMaps::BoundingBox)
      expect(results.first[:area_km2]).to be > 0
    end
  end
end
