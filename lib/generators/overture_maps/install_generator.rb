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
        migration_template "division_migration.rb.tt", "db/migrate/create_overture_divisions.rb"
        migration_template "transportation_migration.rb.tt", "db/migrate/create_overture_transportation.rb"
        migration_template "base_features_migration.rb.tt", "db/migrate/create_overture_base_features.rb"
        migration_template "imported_areas_migration.rb.tt", "db/migrate/create_overture_imported_areas.rb"
      end

      def create_models
        template "category_model.rb.tt", "app/models/overture_category.rb"
        template "place_model.rb.tt", "app/models/overture_place.rb"
        template "building_model.rb.tt", "app/models/overture_building.rb"
        template "address_model.rb.tt", "app/models/overture_address.rb"
        template "division_model.rb.tt", "app/models/overture_division.rb"
        template "segment_model.rb.tt", "app/models/overture_segment.rb"
        template "connector_model.rb.tt", "app/models/overture_connector.rb"
        template "base_feature_model.rb.tt", "app/models/overture_base_feature.rb"
      end
    end
  end
end
