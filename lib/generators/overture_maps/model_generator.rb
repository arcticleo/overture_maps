# frozen_string_literal: true

require "rails/generators/base"
require "rails/generators/named_base"
require "rails/generators/resource_helpers"

module OvertureMaps
  module Generators
    class ModelGenerator < Rails::Generators::NamedBase
      source_root File.expand_path("templates", __dir__)
      argument :name, type: :string, required: true, banner: "model_name"
      class_option :theme, type: :string, default: "places", desc: "Overture theme (places, buildings, addresses, divisions, transportation)"
      class_option :geometry, type: :string, default: "point", desc: "Geometry type (point, polygon, linestring)"
      class_option :migration, type: :boolean, default: true, desc: "Create migration"

      def create_model_file
        template "model.rb.tt", File.join("app/models", "#{file_name}.rb")
      end

      def create_migration_file
        return unless options[:migration]

        migration_template(
          "migration.rb.tt",
          "db/migrate/create_#{table_name}.rb",
          migration_version: migration_version
        )
      end

      private

      def table_name
        file_name.pluralize
      end

      def migration_version
        "[#{Rails::VERSION::MAJOR}.#{Rails::VERSION::MINOR}]"
      end

      def geometry_type
        options[:geometry]
      end
    end
  end
end
