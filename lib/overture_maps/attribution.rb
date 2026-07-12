# frozen_string_literal: true

module OvertureMaps
  # Builds license-correct attribution from the data actually imported. The
  # `sources` column preserved on every row records which upstream datasets
  # contributed, which is what determines the required notices:
  #
  #   OvertureMaps::Attribution.notices  # => array of notice strings
  #   OvertureMaps::Attribution.text     # => single line for a map corner
  #
  # Overture licensing: ODbL for OSM-derived themes (base, buildings,
  # divisions, transportation), CDLA-Permissive-2.0 for places (Foursquare
  # requires referencing their NOTICE), per-source terms for addresses.
  # See https://docs.overturemaps.org/attribution/
  module Attribution
    FOURSQUARE_NOTICE_URL = "https://opensource.foursquare.com/places-notice-txt/"

    class << self
      def notices(models: default_models)
        datasets = dataset_names(models: models)

        notices = ["Overture Maps Foundation — overturemaps.org"]
        notices << "© OpenStreetMap contributors (ODbL)" if datasets.any? { |d| d.match?(/openstreetmap|\bosm\b/i) }
        if datasets.any? { |d| d.match?(/foursquare/i) }
          notices << "Includes Foursquare data (CDLA-Permissive-2.0) — see #{FOURSQUARE_NOTICE_URL}"
        end

        remaining = datasets.reject { |d| d.match?(/openstreetmap|\bosm\b|foursquare/i) }
        notices << "Data sources: #{remaining.sort.join(", ")}" if remaining.any?
        notices
      end

      def text(models: default_models)
        notices(models: models).join(" · ")
      end

      # Distinct upstream dataset names across the given models' sources
      # columns.
      def dataset_names(models: default_models)
        models.flat_map { |model| datasets_for(model) }.uniq
      end

      def datasets_for(model)
        return [] unless model.table_exists? && model.column_names.include?("sources")

        model.connection.select_values(<<~SQL).compact
          SELECT DISTINCT elem->>'dataset'
          FROM #{model.quoted_table_name},
               LATERAL jsonb_array_elements(sources) AS elem
          WHERE sources IS NOT NULL AND jsonb_typeof(sources) = 'array'
        SQL
      rescue StandardError
        []
      end

      private

      def default_models
        [
          Models::Place, Models::Building, Models::Address, Models::Division,
          Models::Segment, Models::Connector, Models::BaseFeature
        ]
      end
    end
  end
end
