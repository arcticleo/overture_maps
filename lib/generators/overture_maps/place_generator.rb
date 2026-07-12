# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class PlaceGenerator < BaseGenerator
      desc "Creates the overture_places table and model"

      def create_migration
        migration_template "place_migration.rb.tt", "db/migrate/create_overture_places.rb"
      end

      def create_model
        template "place_model.rb.tt", "app/models/overture_place.rb"
      end
    end
  end
end
