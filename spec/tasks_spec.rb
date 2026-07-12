# frozen_string_literal: true

require "rake"

RSpec.describe "rake tasks" do
  # Loading the task files twice was the original sin that broke the flagship
  # import command (Rake merges same-named tasks and runs every body). This
  # guards against a regression: every task must be defined exactly once.
  it "defines every task exactly once" do
    rake = Rake::Application.new
    Rake.application = rake

    Dir[File.expand_path("../lib/tasks/*.rake", __dir__)].each { |f| load f }

    duplicates = rake.tasks.select { |t| t.actions.length > 1 }.map(&:name)
    expect(duplicates).to be_empty

    %w[
      overture_maps:import:places
      overture_maps:import:buildings
      overture_maps:import:addresses
      overture_maps:import:all
      overture_maps:import:search
      overture_maps:import:stats
      overture_maps:download:places
      overture_maps:download:bbox
      overture_maps:download:nearby
      overture_maps:download:versions
      overture_maps:download:themes
      overture_maps:categories:populate
    ].each do |name|
      expect(rake.tasks.map(&:name)).to include(name)
    end
  ensure
    Rake.application = nil
  end

  it "does not define helper methods on Object" do
    expect(Object.private_method_defined?(:parse_geometry)).to be(false)
    expect(Object.private_method_defined?(:search_divisions)).to be(false)
    expect(Object.private_method_defined?(:parse_location)).to be(false)
  end
end
