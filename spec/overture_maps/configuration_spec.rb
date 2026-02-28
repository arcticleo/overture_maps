# frozen_string_literal: true

RSpec.describe OvertureMaps::Configuration do
  describe "#initialize" do
    it "sets default values" do
      config = described_class.new

      expect(config.timeout).to eq(30)
    end
  end

  describe "#timeout" do
    it "can be set and read" do
      config = described_class.new
      config.timeout = 60

      expect(config.timeout).to eq(60)
    end
  end
end
