# frozen_string_literal: true

module OvertureMaps
  module Generators
    class BuildingGenerator < ::Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      def create_migration
        migration_template(
          "building_migration.rb.tt",
          "db/migrate/create_overture_buildings.rb"
        )
      end

      def create_model
        template(
          "building_model.rb.tt",
          "app/models/overture_building.rb"
        )
      end
    end
  end
end
