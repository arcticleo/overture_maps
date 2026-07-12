# frozen_string_literal: true

RSpec.describe OvertureMaps::Syncer do
  before do
    allow(OvertureMaps::Releases).to receive(:all).and_return(["2026-06-17.0", "2026-05-21.0"])
    OvertureMaps.configure { |c| c.logger = Logger.new(File::NULL) }
  end

  let(:syncer) { described_class.new(target_release: "2026-06-17.0") }
  let(:bbox) { OvertureMaps::BoundingBox.new(lat1: 47.5, lng1: -122.4, lat2: 47.7, lng2: -122.2, display_name: "seattle") }

  def fake_area(release:, model:)
    double(
      "ImportedArea",
      theme: "places", feature_type: "place", slug: "seattle",
      release: release, model_class: model, to_bounding_box: bbox,
      update!: true
    )
  end

  def fake_model
    deleted = []
    model = double("Model", deleted: deleted)
    relation = double("relation")
    allow(model).to receive(:where) do |conditions|
      deleted.concat(conditions[:id])
      relation
    end
    allow(relation).to receive(:delete_all) { 0 }
    model
  end

  describe "#releases_between" do
    it "returns the steps from current (exclusive) to target (inclusive), oldest first" do
      expect(syncer.releases_between("2026-05-21.0", "2026-06-17.0")).to eq(["2026-06-17.0"])
    end

    it "returns nil when the source release is unknown" do
      expect(syncer.releases_between("2025-01-01.0", "2026-06-17.0")).to be_nil
    end

    it "returns nil when target is not newer" do
      expect(syncer.releases_between("2026-06-17.0", "2026-06-17.0")).to be_nil
    end
  end

  describe "#sync_area" do
    it "reports up-to-date areas without touching anything" do
      area = fake_area(release: "2026-06-17.0", model: fake_model)
      expect(OvertureMaps::Changelog).not_to receive(:removed_ids)

      result = syncer.sync_area(area)
      expect(result.status).to eq(:up_to_date)
    end

    it "applies changelog removals then re-imports when the chain is known" do
      model = fake_model
      area = fake_area(release: "2026-05-21.0", model: model)

      expect(OvertureMaps::Changelog).to receive(:removed_ids)
        .with(theme: "places", type: "place", release: "2026-06-17.0", bbox: anything)
        .and_return(%w[gone-1 gone-2])

      runner = instance_double(OvertureMaps::Import::LocationBasedRunner,
                               run: nil, imported_count: 410, error_count: 0)
      allow(runner).to receive(:run).and_return(runner)
      expect(OvertureMaps::Import::LocationBasedRunner).to receive(:new) do |args|
        expect(args[:release]).to eq("2026-06-17.0")
        expect(args[:models]).to eq({ "place" => model })
        runner
      end

      expect(area).to receive(:update!).with(release: "2026-06-17.0", records_count: 410)

      result = syncer.sync_area(area)
      expect(result.status).to eq(:synced)
      expect(model.deleted).to eq(%w[gone-1 gone-2])
      expect(result.imported).to eq(410)
    end

    it "falls back to a full refresh when the chain is unknown" do
      model = fake_model
      purge_relation = double("purge", delete_all: 55)
      allow(model).to receive(:within_bounds).and_return(purge_relation)
      area = fake_area(release: "2024-01-01.0", model: model)

      runner = instance_double(OvertureMaps::Import::LocationBasedRunner,
                               imported_count: 400, error_count: 0)
      allow(runner).to receive(:run).and_return(runner)
      allow(OvertureMaps::Import::LocationBasedRunner).to receive(:new).and_return(runner)
      expect(OvertureMaps::Changelog).not_to receive(:removed_ids)

      result = syncer.sync_area(area)
      expect(result.status).to eq(:refreshed)
      expect(result.removed).to eq(55)
    end

    it "captures per-area failures without raising" do
      area = fake_area(release: "2026-05-21.0", model: fake_model)
      allow(OvertureMaps::Changelog).to receive(:removed_ids).and_raise(OvertureMaps::Error, "boom")

      result = syncer.sync_area(area)
      expect(result.status).to eq(:failed)
      expect(result.message).to eq("boom")
    end
  end
end
