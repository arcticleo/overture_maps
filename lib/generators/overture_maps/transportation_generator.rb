# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class TransportationGenerator < BaseGenerator
      desc "Creates the overture_segments/overture_connectors tables and models"

      def create_migration
        migration_template "transportation_migration.rb.tt", "db/migrate/create_overture_transportation.rb"
      end

      def create_models
        template "segment_model.rb.tt", "app/models/overture_segment.rb"
        template "connector_model.rb.tt", "app/models/overture_connector.rb"
      end
    end
  end
end
