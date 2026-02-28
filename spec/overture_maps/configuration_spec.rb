# frozen_string_literal: true

RSpec.describe OvertureMaps::Configuration do
  describe "#initialize" do
    it "sets default values" do
      config = described_class.new

      expect(config.api_key).to be_nil
      expect(config.base_url).to eq("https://api.overturemapsapi.com")
      expect(config.timeout).to eq(30)
    end
  end

  describe "#api_key" do
    it "can be set and read" do
      config = described_class.new
      config.api_key = "test-key"

      expect(config.api_key).to eq("test-key")
    end
  end

  describe "#validate!" do
    it "raises error when api_key is nil" do
      config = described_class.new

      expect { config.validate! }
        .to raise_error(OvertureMaps::ConfigurationError, /api_key is required/)
    end

    it "raises error when api_key is empty" do
      config = described_class.new
      config.api_key = ""

      expect { config.validate! }
        .to raise_error(OvertureMaps::ConfigurationError, /api_key is required/)
    end

    it "does not raise when api_key is set" do
      config = described_class.new
      config.api_key = "valid-key"

      expect { config.validate! }.not_to raise_error
    end
  end
end
