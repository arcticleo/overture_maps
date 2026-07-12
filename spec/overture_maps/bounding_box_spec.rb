# frozen_string_literal: true

RSpec.describe OvertureMaps::BoundingBox do
  describe ".parse" do
    it "parses comma-separated coordinates" do
      bbox = described_class.parse("47.606,-122.336,47.609,-122.333")

      expect(bbox.min_lat).to eq(47.606)
      expect(bbox.max_lat).to eq(47.609)
      expect(bbox.min_lng).to eq(-122.336)
      expect(bbox.max_lng).to eq(-122.333)
    end

    it "parses underscore-separated coordinates" do
      bbox = described_class.parse("47.606_-122.336_47.609_-122.333")

      expect(bbox).not_to be_nil
      expect(bbox.min_lat).to eq(47.606)
    end

    it "parses space-separated coordinates" do
      expect(described_class.parse("47.606 -122.336 47.609 -122.333")).not_to be_nil
    end

    it "normalizes corners given in any order" do
      bbox = described_class.parse("47.609,-122.333,47.606,-122.336")

      expect(bbox.min_lat).to eq(47.606)
      expect(bbox.max_lng).to eq(-122.333)
    end

    it "captures a |display_name suffix" do
      bbox = described_class.parse("47.6,-122.3,47.7,-122.2|Greater Seattle")

      expect(bbox.display_name).to eq("Greater Seattle")
      expect(bbox.slug).to eq("greater_seattle")
    end

    it "returns nil for division names" do
      expect(described_class.parse("Seattle")).to be_nil
      expect(described_class.parse("New York")).to be_nil
    end

    it "returns nil for partial coordinates" do
      expect(described_class.parse("47.606,-122.336")).to be_nil
    end

    it "rejects out-of-range coordinates" do
      expect { described_class.parse("95.0,-122.3,47.7,-122.2") }.to raise_error(ArgumentError)
    end
  end

  describe ".around" do
    it "builds a box around a center point" do
      bbox = described_class.around(lat: 47.6, lng: -122.3, radius_meters: 1000)

      expect(bbox.min_lat).to be < 47.6
      expect(bbox.max_lat).to be > 47.6
      expect(bbox.min_lng).to be < -122.3
      expect(bbox.max_lng).to be > -122.3
      # ~1km in degrees latitude is ~0.009
      expect(bbox.max_lat - bbox.min_lat).to be_within(0.001).of(0.018)
    end

    it "clamps to valid ranges near the poles" do
      bbox = described_class.around(lat: 89.9999, lng: 0, radius_meters: 100_000)

      expect(bbox.max_lat).to eq(90)
    end
  end

  describe ".from_overture" do
    it "converts an Overture bbox struct" do
      bbox = described_class.from_overture(
        { "xmin" => -122.4, "xmax" => -122.2, "ymin" => 47.5, "ymax" => 47.7 },
        display_name: "Seattle"
      )

      expect(bbox.min_lng).to eq(-122.4)
      expect(bbox.max_lat).to eq(47.7)
      expect(bbox.slug).to eq("seattle")
    end
  end
end
