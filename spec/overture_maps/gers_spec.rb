# frozen_string_literal: true

RSpec.describe OvertureMaps::GERS do
  describe ".valid_id?" do
    it "accepts dashed UUIDs (current format)" do
      expect(described_class.valid_id?("1ef5ffe6-cea9-4d4d-98f3-efbedfa4a8d7")).to be(true)
    end

    it "accepts 32-char undashed hex (legacy format)" do
      expect(described_class.valid_id?("08b2aa845a1affff0200c75c0eb0d267")).to be(true)
    end

    it "rejects everything else" do
      expect(described_class.valid_id?("not-an-id")).to be(false)
      expect(described_class.valid_id?("1ef5ffe6-cea9-4d4d-98f3")).to be(false)
      expect(described_class.valid_id?(nil)).to be(false)
      expect(described_class.valid_id?(42)).to be(false)
    end
  end

  describe ".lookup" do
    it "rejects malformed ids before querying" do
      expect { described_class.lookup("'; DROP TABLE--") }.to raise_error(ArgumentError, /not a GERS id/)
    end

    it "queries the registry with a bound id" do
      engine = instance_double(OvertureMaps::QueryEngine)
      allow(OvertureMaps::QueryEngine).to receive(:instance).and_return(engine)

      expect(engine).to receive(:query) do |sql, params|
        expect(sql).to include("/registry/*.parquet")
        expect(sql).to include("WHERE id = ?")
        expect(params).to eq(["1ef5ffe6-cea9-4d4d-98f3-efbedfa4a8d7"])
        [{ "id" => "1ef5ffe6-cea9-4d4d-98f3-efbedfa4a8d7", "last_seen" => "2026-06-17.0" }]
      end

      row = described_class.lookup("1ef5ffe6-cea9-4d4d-98f3-efbedfa4a8d7")
      expect(row["last_seen"]).to eq("2026-06-17.0")
    end
  end
end
