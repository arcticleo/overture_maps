# frozen_string_literal: true

require "rails/railtie"

module OvertureMaps
  class Railtie < Rails::Railtie
    rake_tasks do
      Dir[File.join(__dir__, "..", "tasks", "*.rake")].each { |f| load f }
    end

    initializer "overture_maps.configure_rgeo" do
      ActiveSupport.on_load(:active_record) do
        require "rgeo/active_record"

        RGeo::ActiveRecord::SpatialFactoryStore.instance.tap do |store|
          factory = RGeo::Geographic.spherical_factory(srid: 4326)
          store.default = factory
          %w[geometry geometry_collection line_string multi_line_string
             multi_point multi_polygon point polygon].each do |geo_type|
            store.register(factory, geo_type: geo_type)
          end
        end
      end
    end
  end
end
