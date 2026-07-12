# frozen_string_literal: true

module OvertureMaps
  module Import
    # Maps raw Overture records (nested structs from parquet) onto flat model
    # attributes. Every record gets the same key set — heterogeneous keys make
    # ActiveRecord's insert_all/upsert_all reject the whole batch.
    class RecordMapper
      def self.for(theme:, model_class:, type: nil, release: nil)
        new(theme: theme, model_class: model_class, type: type, release: release)
      end

      attr_reader :theme, :type, :model_class, :release

      def initialize(theme:, model_class:, type: nil, release: nil)
        @theme = theme
        @type = type
        @model_class = model_class
        @release = release
        @columns = model_class.column_names
      end

      # Returns attributes with a stable key set, or nil if the record can't
      # be mapped (caller records the error).
      def call(record)
        attrs = base_attributes(record)
        attrs.merge!(theme_attributes(record))
        normalize(attrs)
      end

      # The attribute keys every mapped record will contain.
      def key_set
        @key_set ||= begin
          sample = base_attributes({}).merge(theme_attributes({}))
          keys = sample.keys.select { |k| @columns.include?(k.to_s) }
          keys | %i[id geometry]
        end
      end

      private

      def base_attributes(record)
        {
          id: record["id"],
          geometry: record["geometry"],
          names: record["names"],
          name: record.dig("names", "primary"),
          sources: record["sources"],
          overture_release: release,
          created_at: now,
          updated_at: now
        }
      end

      def theme_attributes(record)
        case theme
        when "places" then place_attributes(record)
        when "buildings" then building_attributes(record)
        when "addresses" then address_attributes(record)
        when "divisions" then division_attributes(record)
        when "transportation" then transportation_attributes(record)
        when "base" then base_theme_attributes(record)
        else {}
        end
      end

      def place_attributes(record)
        {
          # categories is deprecated in favor of basic_category + taxonomy
          # (~Sep 2026); read both shapes so imports survive the cutover.
          categories: record["categories"] || record["taxonomy"],
          primary_category: record.dig("categories", "primary") || record["basic_category"],
          brands: record["brand"],
          addresses: record["addresses"],
          confidence: record["confidence"],
          operating_status: record["operating_status"],
          websites: record["websites"],
          socials: record["socials"],
          emails: record["emails"],
          phones: record["phones"],
          country: first_address_value(record, "country")
        }
      end

      def building_attributes(record)
        {
          subtype: record["subtype"],
          building_class: record["class"],
          height: record["height"],
          num_floors: record["num_floors"],
          level: record["level"],
          is_underground: record["is_underground"]
        }
      end

      def address_attributes(record)
        levels = Array(record["address_levels"])
        {
          number: record["number"],
          street: record["street"],
          unit: record["unit"],
          postcode: record["postcode"],
          country: record["country"],
          postal_city: record["postal_city"],
          address_levels: record["address_levels"],
          # Best-effort convenience columns; ordering of address_levels is
          # country-dependent, the authoritative data is the jsonb column.
          region: levels.dig(0, "value"),
          locality: record["postal_city"] || levels.dig(1, "value")
        }
      end

      # division_area records: the geocodable territory of a division.
      def division_attributes(record)
        bbox = record["bbox"] || {}
        {
          division_id: record["division_id"],
          subtype: record["subtype"],
          division_class: record["class"],
          country: record["country"],
          region: record["region"],
          is_land: record["is_land"],
          is_territorial: record["is_territorial"],
          bbox_xmin: bbox["xmin"],
          bbox_xmax: bbox["xmax"],
          bbox_ymin: bbox["ymin"],
          bbox_ymax: bbox["ymax"]
        }
      end

      def transportation_attributes(record)
        return {} unless type == "segment" # connectors carry only id + geometry

        {
          subtype: record["subtype"],
          segment_class: record["class"],
          subclass: record["subclass"],
          connectors: record["connectors"],
          routes: record["routes"],
          speed_limits: record["speed_limits"],
          access_restrictions: record["access_restrictions"],
          prohibited_transitions: record["prohibited_transitions"],
          road_surface: record["road_surface"],
          road_flags: record["road_flags"],
          destinations: record["destinations"]
        }
      end

      def base_theme_attributes(record)
        {
          feature_type: type,
          subtype: record["subtype"],
          feature_class: record["class"],
          surface: record["surface"],
          elevation: record["elevation"],
          level: record["level"],
          wikidata: record["wikidata"],
          source_tags: record["source_tags"]
        }
      end

      def first_address_value(record, key)
        addresses = record["addresses"]
        return nil unless addresses.is_a?(Array) && addresses.first.is_a?(Hash)

        addresses.first[key]
      end

      # Keep only real columns, ensure every key from the key set is present
      # so batches stay homogeneous.
      def normalize(attrs)
        normalized = {}
        key_set.each { |key| normalized[key] = attrs[key] }
        normalized
      end

      def now
        @now ||= Time.now.utc
      end
    end
  end
end
