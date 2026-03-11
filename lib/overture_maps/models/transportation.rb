# frozen_string_literal: true

module OvertureMaps
  module Models
    class Transportation < Base
      self.table_name = "overture_transportations"

      # Scope: by subtype (road, rail, etc.)
      scope :by_subtype, ->(subtype) { where(subtype: subtype) }

      # Scope: by class
      scope :by_class, ->(klass) { where(class: klass) }

      # Scope: by subclass
      scope :by_subclass, ->(subclass) { where(subclass: subclass) }

      # Scope: roads
      scope :roads, -> { where(subtype: "road") }

      # Scope: rails
      scope :rails, -> { where(subtype: "rail") }

      # Scope: connectors (intersections, entrances)
      scope :connectors, -> { where(subtype: "connector") }

      # Scope: segments (road/rail segments)
      scope :segments, -> { where(subtype: "segment") }

      # Scope: has speed limits
      scope :with_speed_limits, -> { where("speed_limits IS NOT NULL AND jsonb_array_length(speed_limits) > 0") }

      # Scope: has access restrictions
      scope :with_access_restrictions, -> { where("access_restrictions IS NOT NULL AND jsonb_array_length(access_restrictions) > 0") }

      # Extract primary name
      def display_name
        names.first if names.present?
      end

      # Get speed limit at a specific position (for segments)
      def speed_limit_at(position)
        return nil unless speed_limits.is_a?(Array)

        speed_limits.find do |rule|
          range = rule["between"] || rule[:between]
          range && position >= range[0] && position <= range[1]
        end&.[]("value") || speed_limits.find { |rule|
          range = rule["between"] || rule[:between]
          range && position >= range[0] && position <= range[1]
        }&.[]&(:value)
      end

      # Check if access is allowed for a specific access type
      def access_allowed?(access_type)
        return true unless access_restrictions.is_a?(Array) && access_restrictions.any?

        # Check if there's a restriction
        restriction = access_restrictions.find { |r| r["access_type"] == access_type || r[:access_type] == access_type }
        restriction.nil?
      end

      # Get road surface at a specific position
      def surface_at(position)
        return nil unless road_surface.is_a?(Array)

        road_surface.find do |rule|
          range = rule["between"] || rule[:between]
          range && position >= range[0] && position <= range[1]
        end
      end
    end
  end
end
