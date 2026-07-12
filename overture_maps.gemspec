# frozen_string_literal: true

require_relative "lib/overture_maps/version"

Gem::Specification.new do |spec|
  spec.name = "overture_maps"
  spec.version = OvertureMaps::VERSION
  spec.summary = "Ruby gem for Overture Maps integration"
  spec.description = "Download, store, and query Overture Maps geospatial data in your Rails application"
  spec.homepage = "https://github.com/overture-maps/overture-maps-ruby"
  spec.license = "MIT"
  spec.author = "Overture Maps"
  spec.email = "info@overturemaps.org"

  spec.required_ruby_version = ">= 3.0"

  spec.files = Dir["lib/**/*", "README.md", "LICENSE.txt"]
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 7.0"
  spec.add_dependency "railties", ">= 7.0"
  spec.add_dependency "csv"
  spec.add_dependency "rgeo", "~> 3.0"
  spec.add_dependency "rgeo-activerecord", ">= 7.0"
  spec.add_dependency "rgeo-geojson", "~> 2.0"
  spec.add_dependency "activerecord-postgis-adapter", ">= 9.0"
  spec.add_dependency "parquet", ">= 0.5"

  # The duckdb gem (native bindings) is optional but recommended; without it
  # the gem falls back to the DuckDB CLI (PATH lookup, then a pinned,
  # verified download).

  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.18"
  spec.add_development_dependency "rake", ">= 13.0"
end
