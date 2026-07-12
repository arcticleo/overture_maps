# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class InstallGenerator < BaseGenerator
      desc "Creates the PostGIS extension, Overture tables, and model files"

      def create_migrations
        migration_template "postgis_migration.rb.tt", "db/migrate/create_overture_postgis.rb"
        migration_template "category_migration.rb.tt", "db/migrate/create_overture_categories.rb"
        migration_template "place_migration.rb.tt", "db/migrate/create_overture_places.rb"
        migration_template "building_migration.rb.tt", "db/migrate/create_overture_buildings.rb"
        migration_template "address_migration.rb.tt", "db/migrate/create_overture_addresses.rb"
      end

      def create_models
        template "category_model.rb.tt", "app/models/overture_category.rb"
        template "place_model.rb.tt", "app/models/overture_place.rb"
        template "building_model.rb.tt", "app/models/overture_building.rb"
        template "address_model.rb.tt", "app/models/overture_address.rb"
      end
    end
  end
end
