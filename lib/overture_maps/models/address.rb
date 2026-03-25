# frozen_string_literal: true

module OvertureMaps
  module Models
    class Address < Base
      self.table_name = "overture_addresses"

      # Scope: by country
      scope :by_country, ->(country) {
        where(country: country)
      }

      # Scope: by postal_city
      scope :by_postal_city, ->(postal_city) {
        where(postal_city: postal_city)
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
        [street, postal_city, region, country, postcode].compact.join(", ")
      end
    end
  end
end
