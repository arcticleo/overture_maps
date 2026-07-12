# frozen_string_literal: true

RSpec.describe OvertureMaps::Import::Runner do
  # Minimal stand-in for an ActiveRecord model class.
  def fake_model(columns: %w[id geometry name created_at updated_at])
    Class.new do
      define_singleton_method(:column_names) { columns }
      define_singleton_method(:primary_key) { "id" }
      define_singleton_method(:upserted) { @upserted ||= [] }
      define_singleton_method(:upsert_all) do |records, unique_by: nil|
        records.each do |r|
          raise ActiveRecord::RecordNotUnique, "duplicate" if r[:id] == "boom"

          upserted << r
        end
      end
    end
  end

  describe "#import_from_records" do
    it "upserts records in batches" do
      model = fake_model
      runner = described_class.new(model_class: model, theme: "places", batch_size: 2)

      records = [
        { "id" => "1", "geometry" => "POINT (1 1)", "names" => { "primary" => "A" } },
        { "id" => "2", "geometry" => "POINT (2 2)", "names" => { "primary" => "B" } },
        { "id" => "3", "geometry" => "POINT (3 3)", "names" => { "primary" => "C" } }
      ]
      runner.import_from_records(records)

      expect(runner.imported_count).to eq(3)
      expect(runner.error_count).to eq(0)
      expect(model.upserted.map { |r| r[:id] }).to eq(%w[1 2 3])
      expect(model.upserted.first[:geometry]).to be_a(RGeo::Feature::Instance)
    end

    it "isolates bad rows without abandoning the batch" do
      model = fake_model
      runner = described_class.new(model_class: model, theme: "places", batch_size: 10)

      records = [
        { "id" => "1", "geometry" => "POINT (1 1)" },
        { "id" => "boom", "geometry" => "POINT (2 2)" },
        { "id" => "3", "geometry" => "POINT (3 3)" }
      ]
      runner.import_from_records(records)

      expect(runner.imported_count).to eq(2)
      expect(runner.error_count).to eq(1)
      expect(runner.errors.first[:record_id]).to eq("boom")
    end

    it "skips records that fail to map without aborting the loop" do
      model = fake_model
      runner = described_class.new(model_class: model, theme: "places")

      records = [
        { "id" => "1", "geometry" => "not parseable at all {{{" },
        { "id" => "2", "geometry" => "POINT (2 2)" }
      ]
      runner.import_from_records(records)

      expect(runner.imported_count).to eq(1)
      expect(runner.error_count).to eq(1)
    end

    it "applies filters before mapping" do
      model = fake_model
      runner = described_class.new(model_class: model, theme: "places")

      records = [
        { "id" => "1", "geometry" => "POINT (1 1)", "keep" => false },
        { "id" => "2", "geometry" => "POINT (2 2)", "keep" => true }
      ]
      runner.import_from_records(records, filter: ->(r) { r["keep"] })

      expect(model.upserted.map { |r| r[:id] }).to eq(["2"])
    end

    it "caps stored errors but keeps counting" do
      model = fake_model
      runner = described_class.new(model_class: model, theme: "places")

      records = (1..60).map { |i| { "id" => "r#{i}", "geometry" => "{{{bad" } }
      runner.import_from_records(records)

      expect(runner.error_count).to eq(60)
      expect(runner.errors.length).to eq(described_class::MAX_STORED_ERRORS)
    end
  end

  describe "geometry parsing" do
    let(:runner) { described_class.new(model_class: fake_model, theme: "places") }

    def parse(geom)
      runner.send(:parse_geometry, geom)
    end

    it "parses WKT (what DuckDB text output produces)" do
      point = parse("POINT (-122.3 47.6)")

      expect(point.x).to eq(-122.3)
      expect(point.y).to eq(47.6)
    end

    it "parses WKT polygons" do
      polygon = parse("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")

      expect(polygon.geometry_type.type_name).to eq("Polygon")
    end

    it "parses hex WKB" do
      # POINT (1 2) as little-endian WKB hex
      point = parse("0101000000000000000000F03F0000000000000040")

      expect(point.x).to eq(1.0)
      expect(point.y).to eq(2.0)
    end

    it "parses binary WKB (what parquet extracts contain)" do
      binary = ["0101000000000000000000F03F0000000000000040"].pack("H*")
      point = parse(binary)

      expect(point.x).to eq(1.0)
      expect(point.y).to eq(2.0)
    end

    it "parses GeoJSON strings and hashes" do
      expect(parse('{"type":"Point","coordinates":[1.0,2.0]}').y).to eq(2.0)
      expect(parse({ "type" => "Point", "coordinates" => [3.0, 4.0] }).x).to eq(3.0)
    end

    it "returns nil for nil" do
      expect(parse(nil)).to be_nil
    end
  end
end
