# frozen_string_literal: true

require "rails/engine"

module OvertureMaps
  # Mountable engine providing the read-only query API over imported data:
  #
  #   # config/routes.rb (added by the install generator)
  #   mount OvertureMaps::Engine => "/overture"
  #
  # Rake tasks under lib/tasks and the controllers under app/ are picked up
  # by Rails::Engine's conventions — no manual loading here (a second load
  # would define every rake task twice).
  class Engine < ::Rails::Engine
    isolate_namespace OvertureMaps

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
