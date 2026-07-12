# frozen_string_literal: true

# Boots a minimal Rails app with the engine mounted and provisions a
# dedicated PostGIS test database (on the demo's docker-compose instance by
# default). Engine request specs skip when no database is reachable.
module EngineHarness
  DB_NAME = "overture_maps_gem_test"

  DB_CONFIG = {
    adapter: "postgis",
    host: ENV.fetch("OVERTURE_TEST_DB_HOST", "localhost"),
    port: Integer(ENV.fetch("OVERTURE_TEST_DB_PORT", 5433)),
    username: ENV.fetch("OVERTURE_TEST_DB_USER", "overture_demo"),
    password: ENV.fetch("OVERTURE_TEST_DB_PASSWORD", "overture_demo"),
    database: DB_NAME
  }.freeze

  class << self
    def available?
      setup! if @available.nil?
      @available
    end

    def skip_reason
      @skip_reason
    end

    private

    def setup!
      bootstrap_database!
      ActiveRecord::Base.establish_connection(DB_CONFIG)
      connection = ActiveRecord::Base.connection
      connection.enable_extension("postgis")
      create_schema(connection)
      boot_app!
      seed!
      @available = true
    rescue StandardError, LoadError => e
      @skip_reason = "engine test database unavailable (#{e.class}: #{e.message.lines.first&.strip})"
      @available = false
    end

    def bootstrap_database!
      ActiveRecord::Base.establish_connection(DB_CONFIG.merge(database: "overture_demo_development"))
      connection = ActiveRecord::Base.connection
      exists = connection.select_value(
        ActiveRecord::Base.sanitize_sql(["SELECT 1 FROM pg_database WHERE datname = ?", DB_NAME])
      )
      connection.execute("CREATE DATABASE #{connection.quote_table_name(DB_NAME)}") unless exists
    end

    def create_schema(connection)
      return if connection.table_exists?(:overture_places)

      ActiveRecord::Schema.verbose = false
      ActiveRecord::Schema.define do
        create_table :overture_places, id: :string do |t|
          t.string :name
          t.jsonb :names, default: {}
          t.jsonb :categories, default: {}
          t.string :primary_category
          t.jsonb :brands, default: {}
          t.jsonb :sources, default: []
          t.decimal :confidence, precision: 3, scale: 2
          t.string :operating_status
          t.string :country
          t.string :overture_release
          t.timestamps
        end
        add_column :overture_places, :geometry, :st_point, geographic: true, srid: 4326

        create_table :overture_divisions, id: :string do |t|
          t.string :division_id
          t.string :name
          t.jsonb :names, default: {}
          t.string :subtype
          t.string :division_class
          t.string :country
          t.string :region
          t.boolean :is_land
          t.boolean :is_territorial
          t.float :bbox_xmin
          t.float :bbox_xmax
          t.float :bbox_ymin
          t.float :bbox_ymax
          t.jsonb :sources, default: []
          t.string :overture_release
          t.timestamps
        end
        add_column :overture_divisions, :geometry, :geometry, geographic: true, srid: 4326
      end
    end

    def boot_app!
      return if defined?(EngineDummyApp)

      require "rails"
      require "action_controller/railtie"
      require "overture_maps/engine"

      dummy = Class.new(Rails::Application) do
        config.eager_load = false
        config.hosts.clear
        config.secret_key_base = "engine-test-secret"
        config.logger = Logger.new(File::NULL)
        config.log_level = :fatal
      end
      Object.const_set(:EngineDummyApp, dummy)

      Rails.application.initialize!
      Rails.application.routes.draw { mount OvertureMaps::Engine => "/overture" }
    end

    def seed!
      factory = OvertureMaps::GeometryParser.factory
      OvertureMaps::Models::Place.delete_all
      OvertureMaps::Models::Division.delete_all

      [
        ["p-cafe-1", "Pike Street Coffee", "coffee_shop", -122.3400, 47.6090, 0.95],
        ["p-cafe-2", "Elm Coffee Roasters", "coffee_shop", -122.3310, 47.5990, 0.90],
        ["p-museum", "Seattle Art Museum", "art_museum", -122.3380, 47.6070, 0.98]
      ].each do |id, name, category, lng, lat, confidence|
        OvertureMaps::Models::Place.create!(
          id: id, name: name, primary_category: category,
          names: { "primary" => name },
          categories: { "primary" => category, "alternate" => [] },
          confidence: confidence, country: "US", operating_status: "open",
          overture_release: "2026-06-17.0",
          geometry: factory.point(lng, lat)
        )
      end

      OvertureMaps::Models::Division.create!(
        id: "d-seattle", division_id: "div-seattle", name: "Seattle",
        subtype: "locality", country: "US", region: "US-WA",
        bbox_xmin: -122.46, bbox_xmax: -122.22, bbox_ymin: 47.48, bbox_ymax: 47.73,
        overture_release: "2026-06-17.0"
      )
    end
  end
end
