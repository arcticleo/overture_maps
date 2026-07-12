# frozen_string_literal: true

RSpec.describe OvertureMaps::Storage do
  let(:base) { OvertureMaps::Configuration::DEFAULT_S3_HTTP_URL }

  def listing_xml(keys: [], prefixes: [], next_token: nil)
    contents = keys.map { |k, s| "<Contents><Key>#{k}</Key><Size>#{s || 100}</Size></Contents>" }.join
    commons = prefixes.map { |p| "<CommonPrefixes><Prefix>#{p}</Prefix></CommonPrefixes>" }.join
    token = next_token ? "<NextContinuationToken>#{next_token}</NextContinuationToken>" : ""
    "<?xml version=\"1.0\"?><ListBucketResult>#{contents}#{commons}#{token}</ListBucketResult>"
  end

  describe ".list" do
    it "parses objects and common prefixes" do
      stub_request(:get, "#{base}/?list-type=2&prefix=release/&delimiter=/")
        .to_return(body: listing_xml(prefixes: ["release/2026-06-17.0/", "release/2026-05-21.0/"]))

      result = described_class.list(prefix: "release/", delimiter: "/")

      expect(result[:prefixes]).to eq(["release/2026-06-17.0/", "release/2026-05-21.0/"])
      expect(result[:objects]).to be_empty
    end

    it "follows continuation tokens across pages" do
      stub_request(:get, "#{base}/?list-type=2&prefix=x/")
        .to_return(body: listing_xml(keys: [["x/a.parquet", 10]], next_token: "tok"))
      stub_request(:get, "#{base}/?list-type=2&prefix=x/&continuation-token=tok")
        .to_return(body: listing_xml(keys: [["x/b.parquet", 20]]))

      result = described_class.list(prefix: "x/")

      expect(result[:objects].map { |o| o[:key] }).to eq(["x/a.parquet", "x/b.parquet"])
      expect(result[:objects].last[:size]).to eq(20)
    end
  end

  describe ".download_url" do
    it "follows redirects and writes atomically", :aggregate_failures do
      stub_request(:get, "https://example.com/file.zip")
        .to_return(status: 302, headers: { "Location" => "https://cdn.example.com/file.zip" })
      stub_request(:get, "https://cdn.example.com/file.zip")
        .to_return(body: "zip-bytes")

      Dir.mktmpdir do |dir|
        target = File.join(dir, "file.zip")
        described_class.download_url("https://example.com/file.zip", to: target)

        expect(File.read(target)).to eq("zip-bytes")
        expect(File.exist?("#{target}.part")).to be(false)
      end
    end

    it "raises on HTTP errors and leaves no partial file" do
      stub_request(:get, "https://example.com/missing").to_return(status: 404)

      Dir.mktmpdir do |dir|
        target = File.join(dir, "missing")

        expect {
          described_class.download_url("https://example.com/missing", to: target)
        }.to raise_error(described_class::Error, /HTTP 404/)
        expect(File.exist?(target)).to be(false)
      end
    end
  end

  describe ".download" do
    it "skips when the file already has the expected size" do
      Dir.mktmpdir do |dir|
        target = File.join(dir, "data.parquet")
        File.write(target, "12345")

        expect(described_class.download("k/data.parquet", to: target, expected_size: 5)).to eq(:skipped)
      end
    end
  end
end
