# frozen_string_literal: true

RSpec.describe OvertureMaps do
  describe ".configure" do
    it "yields configuration object" do
      OvertureMaps.configure do |config|
        config.api_key = "test-key"
      end

      expect(OvertureMaps.api_key).to eq("test-key")
    end
  end

  describe ".client" do
    it "returns a Client instance" do
      expect(OvertureMaps.client).to be_a(OvertureMaps::Client)
    end

    it "memoizes the client" do
      client1 = OvertureMaps.client
      client2 = OvertureMaps.client

      expect(client1).to equal(client2)
    end
  end

  describe ".reset" do
    it "clears configuration and client" do
      OvertureMaps.configure { |c| c.api_key = "test-key" }
      OvertureMaps.client

      OvertureMaps.reset

      expect(OvertureMaps.api_key).to be_nil
    end
  end
end
