# frozen_string_literal: true

module OvertureMaps
  module Generators
    class AddressGenerator < ::Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      def create_migration
        migration_template(
          "address_migration.rb.tt",
          "db/migrate/create_overture_addresses.rb"
        )
      end

      def create_model
        template(
          "address_model.rb.tt",
          "app/models/overture_address.rb"
        )
      end
    end
  end
end
