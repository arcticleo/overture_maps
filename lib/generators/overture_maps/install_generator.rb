# frozen_string_literal: true

require "rails/generators/base"

module OvertureMaps
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("../templates", __dir__)

      def create_migrations
        puts "Creating Overture Maps migrations..."

        # Get current timestamp for migration
        timestamp = Time.now.strftime("%Y%m%d%H%M%S")

        # Create PostGIS extension migration
        copy_file "postgis_migration.rb.tt", "db/migrate/#{timestamp}_create_overture_postgis.rb"

        # Create categories migration
        copy_file "category_migration.rb.tt", "db/migrate/#{timestamp.succ}_create_overture_categories.rb"

        # Create places migration
        copy_file "place_migration.rb.tt", "db/migrate/#{timestamp.succ.succ}_create_overture_places.rb"

        # Create buildings migration
        copy_file "building_migration.rb.tt", "db/migrate/#{timestamp.succ.succ.succ}_create_overture_buildings.rb"

        # Create addresses migration
        copy_file "address_migration.rb.tt", "db/migrate/#{timestamp.succ.succ.succ.succ}_create_overture_addresses.rb"

        # Create base features migration
        copy_file "base_feature_migration.rb.tt", "db/migrate/#{timestamp.succ.succ.succ.succ.succ}_create_overture_base_features.rb"

        # Create divisions migration
        copy_file "division_migration.rb.tt", "db/migrate/#{timestamp.succ.succ.succ.succ.succ.succ}_create_overture_divisions.rb"

        # Create transportation migration
        copy_file "transportation_migration.rb.tt", "db/migrate/#{timestamp.succ.succ.succ.succ.succ.succ.succ}_create_overture_transportations.rb"
      end

      def create_models
        puts "Creating Overture Maps models..."

        copy_file "category_model.rb.tt", "app/models/overture_category.rb"
        copy_file "place_model.rb.tt", "app/models/overture_place.rb"
        copy_file "building_model.rb.tt", "app/models/overture_building.rb"
        copy_file "address_model.rb.tt", "app/models/overture_address.rb"
        copy_file "base_feature_model.rb.tt", "app/models/overture_base_feature.rb"
        copy_file "division_model.rb.tt", "app/models/overture_division.rb"
        copy_file "transportation_model.rb.tt", "app/models/overture_transportation.rb"
      end
    end
  end
end
