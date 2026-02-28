# frozen_string_literal: true

module OvertureMaps
  module Models
    class Address < Base
      self.table_name = "overture_addresses"

      # Scope: by country
      scope :by_country, ->(country) {
        where(country: country)
      }

      # Scope: by locality
      scope :by_locality, ->(locality) {
        where(locality: locality)
      }

      # Scope: by region
      scope :by_region, ->(region) {
        where(region: region)
      }

      # Scope: by postcode
      scope :by_postcode, ->(postcode) {
        where(postcode: postcode)
      }

      # Full address string
      def full_address
        [street, locality, region, country, postcode].compact.join(", ")
      end
    end
  end
end
