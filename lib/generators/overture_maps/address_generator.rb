# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class AddressGenerator < BaseGenerator
      desc "Creates the overture_addresses table and model"

      def create_migration
        migration_template "address_migration.rb.tt", "db/migrate/create_overture_addresses.rb"
      end

      def create_model
        template "address_model.rb.tt", "app/models/overture_address.rb"
      end
    end
  end
end
