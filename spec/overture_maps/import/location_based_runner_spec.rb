# frozen_string_literal: true

RSpec.describe OvertureMaps::Import::LocationBasedRunner do
  before do
    OvertureMaps.configure do |c|
      c.release = "2026-06-17.0"
      c.logger = Logger.new(File::NULL)
    end
  end

  def fake_model
    Class.new do
      define_singleton_method(:column_names) { %w[id geometry name created_at updated_at] }
      define_singleton_method(:primary_key) { "id" }
      define_singleton_method(:upserted) { @upserted ||= [] }
      define_singleton_method(:upsert_all) { |records, unique_by: nil| upserted.concat(records) }
    end
  end

  describe "location resolution" do
    it "uses bbox strings directly without searching" do
      expect(OvertureMaps::Import::Downloader).not_to receive(:search_divisions)

      runner = described_class.new(
        theme: "places", location: "47.5_-122.4_47.7_-122.2", model_class: fake_model
      )
      allow(runner).to receive(:import_type)
      runner.run

      expect(runner.bbox.min_lat).to eq(47.5)
    end

    it "searches divisions for names and picks the first without a callback" do
      bbox = OvertureMaps::BoundingBox.new(lat1: 47, lng1: -123, lat2: 48, lng2: -122)
      allow(OvertureMaps::Import::Downloader).to receive(:search_divisions).and_return(
        [{ name: "Seattle", subtype: "locality", country: "US", region: "US-WA", bbox: bbox }]
      )

      runner = described_class.new(theme: "places", location: "Seattle", model_class: fake_model)
      allow(runner).to receive(:import_type)
      runner.run

      expect(runner.bbox).to eq(bbox)
    end

    it "raises CancelledError when the selection callback returns nil" do
      bbox = OvertureMaps::BoundingBox.new(lat1: 47, lng1: -123, lat2: 48, lng2: -122)
      allow(OvertureMaps::Import::Downloader).to receive(:search_divisions).and_return(
        [
          { name: "Springfield", subtype: "locality", bbox: bbox },
          { name: "Springfield", subtype: "county", bbox: bbox }
        ]
      )

      runner = described_class.new(
        theme: "places", location: "Springfield", model_class: fake_model,
        select_division: ->(_results) { nil }
      )

      expect { runner.run }.to raise_error(OvertureMaps::CancelledError)
    end

    it "raises when nothing matches" do
      allow(OvertureMaps::Import::Downloader).to receive(:search_divisions).and_return([])

      runner = described_class.new(theme: "places", location: "Nowhereville", model_class: fake_model)

      expect { runner.run }.to raise_error(OvertureMaps::Error, /no divisions found/i)
    end
  end

  describe "category filtering" do
    it "matches primary and alternate leaf categories" do
      runner = described_class.new(
        theme: "places", location: "0,0,1,1", model_class: fake_model, categories: ["cafe"]
      )
      filter = runner.send(:category_filter)

      expect(filter.call("categories" => { "primary" => "cafe" })).to be(true)
      expect(filter.call("categories" => { "primary" => "bar", "alternate" => ["cafe"] })).to be(true)
      expect(filter.call("categories" => { "primary" => "bar" })).to be(false)
      expect(filter.call("categories" => nil)).to be(false)
    end
  end
end
