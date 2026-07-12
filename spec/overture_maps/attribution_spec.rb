# frozen_string_literal: true

RSpec.describe OvertureMaps::Attribution do
  def fake_model(datasets: [], has_sources: true, exists: true)
    model = double("Model", table_exists?: exists,
                            column_names: has_sources ? %w[id sources] : %w[id])
    connection = double("connection")
    allow(model).to receive(:connection).and_return(connection)
    allow(model).to receive(:quoted_table_name).and_return('"t"')
    allow(connection).to receive(:select_values).and_return(datasets)
    model
  end

  describe ".notices" do
    it "always credits Overture and lists dataset names" do
      models = [fake_model(datasets: ["meta", "Microsoft"])]

      notices = described_class.notices(models: models)

      expect(notices.first).to include("Overture Maps Foundation")
      expect(notices.last).to eq("Data sources: Microsoft, meta")
    end

    it "adds the OSM ODbL notice when OSM contributed" do
      models = [fake_model(datasets: ["OpenStreetMap", "meta"])]

      notices = described_class.notices(models: models)

      expect(notices).to include("© OpenStreetMap contributors (ODbL)")
      expect(notices.last).to eq("Data sources: meta")
    end

    it "references the Foursquare NOTICE when Foursquare contributed" do
      models = [fake_model(datasets: ["Foursquare Places"])]

      notices = described_class.notices(models: models)

      expect(notices.join).to include("opensource.foursquare.com")
    end

    it "skips models without tables or sources columns" do
      models = [fake_model(exists: false), fake_model(has_sources: false)]

      expect(described_class.dataset_names(models: models)).to eq([])
      expect(described_class.notices(models: models))
        .to eq(["Overture Maps Foundation — overturemaps.org"])
    end
  end

  describe ".text" do
    it "joins notices into one line" do
      models = [fake_model(datasets: ["OpenStreetMap"])]

      expect(described_class.text(models: models))
        .to eq("Overture Maps Foundation — overturemaps.org · © OpenStreetMap contributors (ODbL)")
    end
  end
end
