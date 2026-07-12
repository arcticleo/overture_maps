# frozen_string_literal: true

module OvertureMaps
  module Import
    class Error < OvertureMaps::Error; end
  end
end

require "overture_maps/import/downloader"
require "overture_maps/import/parquet_reader"
require "overture_maps/import/record_mapper"
require "overture_maps/import/runner"
require "overture_maps/import/location_based_runner"

module OvertureMaps
  module Import
    # Programmatic file import. The transform may be given as a keyword or a
    # block; it receives each raw record and returns attributes (or nil to
    # skip the record).
    def self.run!(theme:, model_class:, file_path:, batch_size: nil, transform: nil, release: nil, &block)
      transform ||= block
      runner = Runner.new(model_class: model_class, theme: theme, batch_size: batch_size, release: release)
      runner.import_from_file(file_path, theme: theme, transform: transform)
      runner
    end
  end
end
