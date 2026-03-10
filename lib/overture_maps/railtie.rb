require "rails"
require "rgeo/active_record"

module OvertureMaps
  class Railtie < Rails::Railtie
    rake_tasks do
      Dir[File.join(File.dirname(__FILE__), "..", "tasks", "*.rake")].each do |f|
        load f
      end
    end

    initializer "overture_maps.configure" do
      # Configure RGeo for PostGIS using SpatialFactoryStore (rgeo-activerecord 8.x)
      RGeo::ActiveRecord::SpatialFactoryStore.instance.tap do |config|
        config.default = RGeo::Geographic.spherical_factory(srid: 4326)
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "geometry")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "geometry_collection")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "line_string")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "multi_line_string")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "multi_point")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "multi_polygon")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "point")
        config.register(RGeo::Geographic.spherical_factory(srid: 4326), geo_type: "polygon")
      end
    end
  end
end
