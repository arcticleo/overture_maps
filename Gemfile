source "https://rubygems.org"

gemspec

group :development, :test do
  # Native DuckDB bindings (optional at runtime; exercised in specs when present).
  # CI skips the native gem and uses the DuckDB CLI instead.
  gem "duckdb" unless ENV["SKIP_DUCKDB_GEM"]
  # Engine request specs run against a real PostGIS (host apps bring their own driver)
  gem "pg"
  # Official MCP SDK — optional at runtime, exercised by the MCP server specs
  gem "mcp"
end
