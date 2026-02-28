# OvertureMaps Gem Specification
require_relative "lib/overture_maps/version"

Gem::Specification.new do |spec|
  spec.name = "overture_maps"
  spec.version = OvertureMaps::VERSION
  spec.summary = "Ruby gem for Overture Maps integration"
  spec.description = "A Ruby gem to integrate with Overture Maps - query the API and import geospatial data"
  spec.homepage = "https://github.com/overture-maps/overture-maps-ruby"
  spec.license = "MIT"
  spec.author = "Overture Maps"
  spec.email = "info@overturemaps.org"

  spec.required_ruby_version = ">= 3.0"

  spec.files = Dir["lib/**/*", "config/**/*", "README.md", "LICENSE.txt"]
  spec.require_paths = ["lib"]

  # Dependencies
  spec.add_dependency "rails", ">= 7.0"
  spec.add_dependency "faraday", "~> 2.0"
  spec.add_dependency "rgeo", "~> 3.0"
  spec.add_dependency "rgeo-activerecord", "~> 8.0"

  # Optional dependencies for Parquet import (add these manually for cloud storage support)
  # spec.add_dependency "parquet", "~> 0.0" # Apache Parquet Ruby bindings
  # spec.add_dependency "aws-sdk-s3", "~> 1.0" # For S3 access
  # spec.add_dependency "azure-storage-blob", "~> 12.0" # For Azure Blob access
  # spec.add_dependency "duckdb", "~> 0.10" # For spatial queries with DuckDB

  # Development dependencies
  spec.add_development_dependency "rspec", "~> 3.12"
  spec.add_development_dependency "webmock", "~> 3.18"
  spec.add_development_dependency "vcr", "~> 6.1"
end
