# frozen_string_literal: true

require "parquet"

module OvertureMaps
  module Import
    class Error < StandardError; end

    def self.run!(theme:, model_class:, file_path:, batch_size: 1000, transform: nil)
      reader = ParquetReader.new(theme: theme)
      runner = Runner.new(model_class: model_class, batch_size: batch_size)

      puts "Starting import of #{theme}..."
      puts "Source: #{file_path}"
      puts "Total records: #{reader.record_count(source: file_path)}"

      runner.import_from_file(file_path, transform: transform)

      puts "\nImport complete!"
      puts "Imported: #{runner.imported_count}"
      puts "Errors: #{runner.error_count}"

      if runner.errors.any?
        puts "\nErrors encountered:"
        runner.errors.first(10).each do |err|
          puts "  - #{err[:error]}"
        end
      end

      runner
    end
  end
end
