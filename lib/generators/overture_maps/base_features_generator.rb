# frozen_string_literal: true

require "generators/overture_maps/base_generator"

module OvertureMaps
  module Generators
    class BaseFeaturesGenerator < BaseGenerator
      desc "Creates the overture_base_features table and model"

      def create_migration
        migration_template "base_features_migration.rb.tt", "db/migrate/create_overture_base_features.rb"
      end

      def create_model
        template "base_feature_model.rb.tt", "app/models/overture_base_feature.rb"
      end
    end
  end
end
