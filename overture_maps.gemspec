# frozen_string_literal: true

require_relative "lib/overture_maps/version"

Gem::Specification.new do |spec|
  spec.name = "overture_maps"
  spec.version = OvertureMaps::VERSION
  spec.summary = "Overture Maps data for Ruby and Rails"
  spec.description = "Download, import, query, sync, and serve Overture Maps open geospatial data: " \
                     "bbox-filtered GeoParquet extracts via DuckDB, PostGIS imports with GERS " \
                     "changelog syncing, an ad-hoc query API, a mountable REST/MVT engine, and an " \
                     "MCP server. Unofficial community gem, not affiliated with the Overture Maps Foundation."
  spec.homepage = "https://github.com/arcticleo/overture_maps"
  spec.license = "MIT"
  spec.author = "Michael Edlund"
  spec.email = "medlund@mac.com"

  spec.metadata = {
    "homepage_uri" => spec.homepage,
    "source_code_uri" => spec.homepage,
    "changelog_uri" => "#{spec.homepage}/blob/main/CHANGELOG.md",
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "rubygems_mfa_required" => "true"
  }

  spec.required_ruby_version = ">= 3.2"

  spec.files = Dir["lib/**/*", "app/**/*", "config/routes.rb", "exe/*",
                   "README.md", "CHANGELOG.md", "LICENSE.txt"]
  spec.bindir = "exe"
  spec.executables = ["overture-maps-mcp"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 7.0"
  spec.add_dependency "railties", ">= 7.0"
  spec.add_dependency "csv"
  spec.add_dependency "rexml"
  spec.add_dependency "rgeo", "~> 3.0"
  spec.add_dependency "rgeo-activerecord", ">= 7.0"
  spec.add_dependency "rgeo-geojson", "~> 2.0"
  spec.add_dependency "activerecord-postgis-adapter", ">= 9.0"
  spec.add_dependency "parquet", ">= 0.5"

  # Optional (soft-required with a helpful message when missing):
  # - duckdb: native DuckDB bindings; falls back to the DuckDB CLI
  # - mcp: only needed for the overture-maps-mcp executable

  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.18"
  spec.add_development_dependency "rake", ">= 13.0"
  spec.add_development_dependency "rack-test", ">= 2.0"
end
