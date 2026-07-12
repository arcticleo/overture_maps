# frozen_string_literal: true

RSpec.describe OvertureMaps::Releases do
  let(:base) { OvertureMaps::Configuration::DEFAULT_S3_HTTP_URL }

  describe ".all" do
    it "reads releases from the STAC catalog, newest first" do
      stub_request(:get, described_class::STAC_CATALOG_URL).to_return(
        body: {
          links: [
            { rel: "child", href: "./2026-05-21.0/catalog.json" },
            { rel: "child", href: "./2026-06-17.0/catalog.json" },
            { rel: "self", href: "./catalog.json" }
          ]
        }.to_json
      )

      expect(described_class.all).to eq(["2026-06-17.0", "2026-05-21.0"])
    end

    it "falls back to listing the bucket when STAC is unavailable" do
      stub_request(:get, described_class::STAC_CATALOG_URL).to_return(status: 500)
      stub_request(:get, "#{base}/?list-type=2&prefix=release/&delimiter=/").to_return(
        body: <<~XML
          <?xml version="1.0"?>
          <ListBucketResult>
            <CommonPrefixes><Prefix>release/2026-04-16.0/</Prefix></CommonPrefixes>
            <CommonPrefixes><Prefix>release/2026-06-17.0/</Prefix></CommonPrefixes>
          </ListBucketResult>
        XML
      )

      expect(described_class.all).to eq(["2026-06-17.0", "2026-04-16.0"])
      expect(described_class.latest).to eq("2026-06-17.0")
    end
  end

  describe ".current" do
    it "prefers the configured release" do
      OvertureMaps.configure { |c| c.release = "2025-12-17.0" }

      expect(described_class.current).to eq("2025-12-17.0")
    end

    it "rejects malformed release strings" do
      OvertureMaps.configure { |c| c.release = "latest'; DROP" }

      expect { described_class.current }.to raise_error(described_class::Error, /invalid release/)
    end
  end
end
