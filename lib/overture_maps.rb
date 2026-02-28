# OvertureMaps - Ruby gem for Overture Maps integration
require "rgeo"
require "rgeo/active_record"
require "overture_maps/version"
require "overture_maps/configuration"
require "overture_maps/models"
require "overture_maps/models/base"
require "overture_maps/models/place"
require "overture_maps/models/building"
require "overture_maps/models/address"
require "overture_maps/import"
require "overture_maps/import/downloader"

module OvertureMaps
  class Error < StandardError; end
  class ConfigurationError < Error; end

  class << self
    def configure
      yield(configuration)
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def reset
      @configuration = nil
    end
  end
end
