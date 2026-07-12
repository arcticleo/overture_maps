# frozen_string_literal: true

module OvertureMaps
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class CancelledError < Error; end

  class << self
    def configure
      yield(configuration)
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def reset
      @configuration = nil
      Releases.reset!
      QueryEngine.reset!
    end
  end
end

require "overture_maps/version"
require "overture_maps/configuration"
require "overture_maps/util"
require "overture_maps/bounding_box"
require "overture_maps/storage"
require "overture_maps/releases"
require "overture_maps/query_engine"
require "overture_maps/database"
require "overture_maps/models"
require "overture_maps/import"
require "overture_maps/railtie" if defined?(Rails::Railtie)
