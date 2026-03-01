require "rails"
require "rgeo/active_record"

module OvertureMaps
  class Railtie < Rails::Railtie
    initializer "overture_maps.configure" do
      # Configure RGeo for PostGIS
      RGeo::ActiveRecord.configure do |config|
        config.geographic_factory_srid = 4326
      end
    end

    initializer "overture_maps.configure_timeout" do |app|
      OvertureMaps.configure do |config|
        config.timeout = app.config.overture_maps&.timeout || 30
      end
    end
  end
end
