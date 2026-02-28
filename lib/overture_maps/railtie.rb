require "rails"

module OvertureMaps
  class Railtie < Rails::Railtie
    initializer "overture_maps.configure" do |app|
      OvertureMaps.configure do |config|
        config.api_key = app.config.overture_maps&.api_key
        config.base_url = app.config.overture_maps&.base_url || "https://api.overturemapsapi.com"
      end
    end
  end
end
