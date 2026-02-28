# frozen_string_literal: true

require "rgeo/active_record"

module OvertureMaps
  module Models
    def self.setup
      RGeo::ActiveRecord.configure do |config|
        config.geographic_factory_srid = 4326
      end
    end
  end
end
