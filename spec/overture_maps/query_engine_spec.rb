# frozen_string_literal: true

RSpec.describe OvertureMaps::QueryEngine do
  let(:engine) { described_class.instance }

  describe "#interpolate (CLI literal quoting)" do
    it "quotes strings by doubling single quotes" do
      sql = engine.send(:interpolate, "SELECT ? AS q", ["O'Fallon"])

      expect(sql).to eq("SELECT 'O''Fallon' AS q")
    end

    it "does not let injection escape the literal" do
      sql = engine.send(:interpolate, "WHERE name ILIKE ?", ["%'; COPY (SELECT 1) TO '/tmp/pwn'; --"])

      expect(sql).to eq("WHERE name ILIKE '%''; COPY (SELECT 1) TO ''/tmp/pwn''; --'")
    end

    it "renders numbers bare and nil as NULL" do
      expect(engine.send(:interpolate, "a ? b ? c ?", [1.5, nil, 42])).to eq("a 1.5 b NULL c 42")
    end

    it "raises on arity mismatches" do
      expect { engine.send(:interpolate, "?", []) }.to raise_error(described_class::Error)
      expect { engine.send(:interpolate, "x", [1]) }.to raise_error(described_class::Error)
    end
  end

  describe "#build_copy_sql" do
    it "builds parquet copy statements with a quoted path" do
      sql = engine.send(:build_copy_sql, "SELECT 1", "/tmp/out's.parquet", "parquet")

      expect(sql).to eq("COPY (SELECT 1) TO '/tmp/out''s.parquet' (FORMAT PARQUET)")
    end

    it "rejects unknown formats" do
      expect { engine.send(:build_copy_sql, "SELECT 1", "/tmp/x", "csv!") }.to raise_error(ArgumentError)
    end
  end

  describe "#init_statements" do
    it "rejects a malformed s3_region" do
      OvertureMaps.configure { |c| c.s3_region = "us-west-2'; DROP" }

      expect { engine.send(:init_statements) }.to raise_error(described_class::Error)
    end
  end

  describe "backend integration", :duckdb do
    before do
      skip "no DuckDB available" unless engine.native? || described_class.cli_available?
    end

    it "runs a simple query and returns row hashes" do
      allow(WebMock).to receive(:disable_net_connect!) # not needed; local query
      WebMock.allow_net_connect!
      rows = engine.query("SELECT 1 AS one, 'two' AS two", [])

      expect(rows).to eq([{ "one" => 1, "two" => "two" }])
    ensure
      WebMock.disable_net_connect!
    end

    it "binds parameters" do
      WebMock.allow_net_connect!
      rows = engine.query("SELECT ? AS name", ["O'Fallon"])

      expect(rows.first["name"]).to eq("O'Fallon")
    ensure
      WebMock.disable_net_connect!
    end
  end
end
