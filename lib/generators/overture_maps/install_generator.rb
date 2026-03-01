# frozen_string_literal: true

require "rails/generators/base"
require "rails/generators/migration"

module OvertureMaps
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include Rails::Generators::Migration

      source_root File.expand_path("../templates", __dir__)

      def create_migrations
        puts "Creating Overture Maps migrations..."

        # Create PostGIS extension migration first
        migration_template(
          "postgis_migration.rb.tt",
          "db/migrate/create_overture_postgis.rb"
        )

        # Create categories table first (referenced by places)
        migration_template(
          "category_migration.rb.tt",
          "db/migrate/create_overture_categories.rb"
        )

        # Create all migrations
        migration_template(
          "place_migration.rb.tt",
          "db/migrate/create_overture_places.rb"
        )

        migration_template(
          "building_migration.rb.tt",
          "db/migrate/create_overture_buildings.rb"
        )

        migration_template(
          "address_migration.rb.tt",
          "db/migrate/create_overture_addresses.rb"
        )
      end

      def create_models
        puts "Creating Overture Maps models..."

        template(
          "category_model.rb.tt",
          "app/models/overture_category.rb"
        )

        template(
          "place_model.rb.tt",
          "app/models/overture_place.rb"
        )

        template(
          "building_model.rb.tt",
          "app/models/overture_building.rb"
        )

        template(
          "address_model.rb.tt",
          "app/models/overture_address.rb"
        )
      end

      def show_readme
        readme "README.md" if File.exist?(destination_path("README.md"))
      end
    end
  end
end
