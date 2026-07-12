# frozen_string_literal: true

# This gem is an ActiveRecord integration at heart, so the model layer loads
# eagerly. (Deferring via on_load(:active_record) breaks rake tasks: Zeitwerk
# can autoload an app's OverturePlace subclass before anything has touched
# ActiveRecord::Base, at which point the gem superclass wouldn't exist yet.)
require "active_record"
require "rgeo/active_record"
require "activerecord-postgis-adapter"

module OvertureMaps
  module Models
  end
end

require "overture_maps/models/base"
require "overture_maps/models/place"
require "overture_maps/models/building"
require "overture_maps/models/address"
require "overture_maps/models/category"
require "overture_maps/models/division"
require "overture_maps/models/segment"
require "overture_maps/models/connector"
require "overture_maps/models/base_feature"
require "overture_maps/models/imported_area"
