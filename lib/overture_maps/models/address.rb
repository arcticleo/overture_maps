# frozen_string_literal: true

module OvertureMaps
  module Models
    class Address < Base
      self.table_name = "overture_addresses"

      scope :by_country, ->(country) {
        where(country: country)
      }

      scope :by_locality, ->(locality) {
        where(locality: locality)
      }

      scope :by_region, ->(region) {
        where(region: region)
      }

      scope :by_postcode, ->(postcode) {
        where(postcode: postcode)
      }

      def full_address
        [
          [number, street].compact.join(" "),
          unit, locality, region, postcode, country
        ].compact.reject(&:empty?).join(", ")
      end
    end
  end
end
