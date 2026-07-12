source "https://rubygems.org"

gemspec

group :development, :test do
  # Native DuckDB bindings (optional at runtime; exercised in specs when present)
  gem "duckdb"
  # Engine request specs run against a real PostGIS (host apps bring their own driver)
  gem "pg"
end
