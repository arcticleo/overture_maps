require "rails"

module OvertureMaps
  class Railtie < Rails::Railtie
    initializer "overture_maps.configure" do |app|
      OvertureMaps.configure do |config|
        config.timeout = app.config.overture_maps&.timeout || 30
      end
    end
  end
end
