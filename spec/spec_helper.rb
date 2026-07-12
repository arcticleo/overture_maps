# frozen_string_literal: true

require "bundler/setup"
require "active_record"
require "overture_maps"
require "webmock/rspec"
require "tmpdir"
require "logger"

# The gem defers model definitions to the :active_record load hook, which
# fires when ActiveRecord::Base is first referenced.
ActiveRecord::Base

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.example_status_persistence_file_path = "spec/examples.txt"
  config.disable_monkey_patching!
  config.warnings = false

  config.after do
    OvertureMaps.reset
  end

  if config.files_to_run.one?
    config.default_formatter = "doc"
  end
end
