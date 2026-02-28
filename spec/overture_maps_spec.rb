# frozen_string_literal: true

RSpec.describe OvertureMaps do
  describe ".configure" do
    it "yields configuration object" do
      OvertureMaps.configure do |config|
        config.timeout = 60
      end

      expect(OvertureMaps.configuration.timeout).to eq(60)
    end
  end

  describe ".reset" do
    it "clears configuration" do
      OvertureMaps.configure { |c| c.timeout = 60 }

      OvertureMaps.reset

      expect(OvertureMaps.configuration.timeout).to eq(30)
    end
  end
end
