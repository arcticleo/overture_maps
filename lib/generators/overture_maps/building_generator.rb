# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class BuildingGenerator < BaseGenerator
      desc "Creates the overture_buildings table and model"

      def create_migration
        migration_template "building_migration.rb.tt", "db/migrate/create_overture_buildings.rb"
      end

      def create_model
        template "building_model.rb.tt", "app/models/overture_building.rb"
      end
    end
  end
end
