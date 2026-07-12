# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class DivisionGenerator < BaseGenerator
      desc "Creates the overture_divisions table and model"

      def create_migration
        migration_template "division_migration.rb.tt", "db/migrate/create_overture_divisions.rb"
      end

      def create_model
        template "division_model.rb.tt", "app/models/overture_division.rb"
      end
    end
  end
end
