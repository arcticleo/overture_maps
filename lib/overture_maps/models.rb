# frozen_string_literal: true

require "active_support/lazy_load_hooks"

module OvertureMaps
  module Models
  end
end

# Defining the model classes forces ActiveRecord (and the PostGIS adapter)
# to load, so defer until the host app loads ActiveRecord itself.
ActiveSupport.on_load(:active_record) do
  require "rgeo/active_record"
  require "activerecord-postgis-adapter"

  require "overture_maps/models/base"
  require "overture_maps/models/place"
  require "overture_maps/models/building"
  require "overture_maps/models/address"
  require "overture_maps/models/category"
  require "overture_maps/models/division"
  require "overture_maps/models/segment"
  require "overture_maps/models/connector"
  require "overture_maps/models/base_feature"
end
