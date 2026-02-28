# frozen_string_literal: true

module OvertureMaps
  module Generators
    class PlaceGenerator < ::Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      def create_migration
        migration_template(
          "place_migration.rb.tt",
          "db/migrate/create_overture_places.rb"
        )
      end

      def create_model
        template(
          "place_model.rb.tt",
          "app/models/overture_place.rb"
        )
      end
    end
  end
end
