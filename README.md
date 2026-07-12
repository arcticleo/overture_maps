# Overture Maps Ruby Gem

A Ruby gem for integrating with [Overture Maps](https://overturemaps.org/) — an open map data foundation providing geospatial data. Download bbox-filtered extracts of Overture GeoParquet data, import them into your Rails application's PostGIS database, serve them through ActiveRecord, a mountable REST API, or vector tiles, and keep them current with changelog-driven syncs. Or skip the database entirely: query the bucket ad hoc from Ruby, or hand AI assistants the bundled MCP server.

This is an unofficial community gem, not a product of the Overture Maps Foundation.

## Features

- **Location-based import**: `rails overture_maps:import:places[Seattle]` — searches Overture divisions by name, downloads a bbox-filtered extract, and upserts it into PostGIS
- **All six Overture themes**: places, buildings, addresses, divisions, transportation, and base, with category filters backed by the full ~2,100-entry Overture taxonomy
- **Efficient remote access**: DuckDB pushes bbox predicates down to parquet row-group statistics, so only the relevant slice of Overture's multi-hundred-GB datasets is transferred
- **Idempotent imports**: rows are upserted by GERS id; re-running an import updates rather than fails
- **Release-aware**: discovers the latest monthly Overture release via the official STAC catalog; extracts are cached per release and area
- **Changelog-driven sync**: `rails overture_maps:sync` brings imported areas to the latest release by applying the official GERS changelog — no full reloads
- **REST API**: a mountable engine serves imported data as JSON, GeoJSON, and Mapbox Vector Tiles, so MapLibre renders straight from your Rails app with no separate tile server
- **Ad-hoc querying**: `OvertureMaps.query` counts, streams, and exports Overture data straight from the bucket — no import, no database
- **MCP server**: `overture-maps-mcp` gives AI assistants read-only geocoding, query, and GeoJSON export tools over Overture's public data
- **Rails integration**: model generators, spatial scopes, rake tasks, PostGIS utilities, RGeo geometries
- **Attribution helpers**: builds the ODbL/CDLA notices your map is required to display from the imported source metadata

## Installation

Add to your Rails application's Gemfile:

```ruby
gem "overture_maps"

# Recommended: native DuckDB bindings for the fastest remote queries.
# Without this the gem falls back to the duckdb CLI (PATH lookup, then a
# pinned download to ~/.cache/overture_maps).
gem "duckdb"
```

Then run:

```bash
bundle install
```

## Getting Started

### Generate Models and Migrations

```bash
rails generate overture_maps:install
rails db:migrate
```

This creates migrations for the PostGIS extension and the `overture_places`, `overture_buildings`, `overture_addresses`, `overture_categories`, `overture_divisions`, `overture_segments`/`overture_connectors`, and `overture_base_features` tables, plus their model files. Individual generators also exist: `overture_maps:place`, `overture_maps:building`, `overture_maps:address`, `overture_maps:division`, `overture_maps:transportation`, `overture_maps:base_features`.

### Fetch Categories (Recommended)

```bash
rails overture_maps:categories:populate   # ~2,100 categories from the Overture taxonomy
rails overture_maps:categories:list       # list all
rails overture_maps:categories:primary    # list primary categories
```

## Importing Data

```bash
# By location name (searches Overture divisions; prompts if ambiguous)
rails overture_maps:import:places[Seattle]
rails "overture_maps:import:places[New York]"

# By bounding box (underscore-separated lat/lng pairs)
rails overture_maps:import:places[47.606_-122.336_47.609_-122.333]

# Filter places by Overture leaf categories
rails "overture_maps:import:places[Seattle,cafe]"
rails "overture_maps:import:places[Seattle,cafe restaurant]"

# Filter by taxonomy group (expands to leaf categories once
# overture_maps:categories:populate has run)
rails "overture_maps:import:places[Seattle,eat_and_drink]"

# Other themes
rails overture_maps:import:buildings[Seattle]
rails overture_maps:import:addresses[Seattle]
rails overture_maps:import:divisions[Washington]
rails overture_maps:import:transportation[Seattle]
rails overture_maps:import:base[Seattle]

# Everything (resolves the location once, then imports each theme)
rails overture_maps:import:all[Seattle]
```

Importing divisions has a bonus: once `overture_divisions` is populated,
location-name searches resolve against your local database instead of
querying Overture's bucket — imports and searches get much faster.

How it works:

1. The location is parsed as a bounding box, or matched against Overture division areas by name (you're prompted when several match).
2. A bbox-filtered parquet extract is downloaded via DuckDB into `tmp/overture/`, named by theme, type, release, and area — reruns for the same release and area reuse it (you're prompted; non-interactive runs reuse automatically).
3. Records are upserted in batches, keyed on their GERS id.

Useful companions:

```bash
rails overture_maps:import:search[Seattle]   # see matching divisions first
rails overture_maps:import:stats             # row counts per table
OVERTURE_RELEASE=2026-05-21.0 rails overture_maps:import:places[Seattle]  # pin a release
OVERTURE_NON_INTERACTIVE=1 ...               # never prompt (jobs/CI)
IGNORE_ERRORS=1 ...                          # exit 0 despite row errors
VERBOSE=1 ...                                # print row error details
```

## Downloading Data (without importing)

```bash
# Bbox extracts for a location, one file per feature type
rails overture_maps:download:places[Seattle]
rails overture_maps:download:buildings[47.606_-122.336_47.609_-122.333]

# Explicit bbox / point + radius
rails overture_maps:download:bbox[places,47.606,-122.336,47.609,-122.333]
rails overture_maps:download:nearby[places,47.6062,-122.3321,5000]

# Export formats other than parquet
rails "overture_maps:download:bbox[places,47.6,-122.4,47.7,-122.2,place,,,geojson]"

# Complete theme files (no location argument — very large!)
rails overture_maps:download:places

# Discovery
rails overture_maps:download:versions          # available Overture releases
rails overture_maps:download:themes            # themes and their feature types
rails overture_maps:download:types[buildings]  # types present in the current release
rails overture_maps:download:list[places]      # files without downloading
rails overture_maps:download:search_divisions[Seattle]
```

### Data Structure

Overture Maps data is organized by **theme** and **type**:

| Theme | Types |
|-------|-------|
| addresses | address |
| base | bathymetry, infrastructure, land, land_cover, land_use, water |
| buildings | building, building_part |
| divisions | division, division_area, division_boundary |
| places | place |
| transportation | connector, segment |

All six themes can be imported and downloaded. Imports cover the types shown above except `building_part` (planned; needs the parent-building relationship). Division imports use `division_area` — the geocodable territories.

## REST API

The gem ships a mountable engine serving imported data as JSON, GeoJSON,
and Mapbox Vector Tiles (the install generator adds the mount line):

```ruby
# config/routes.rb
mount OvertureMaps::Engine => "/overture"
```

```
GET /overture/places?bbox=-122.35,47.60,-122.33,47.62      # west,south,east,north
GET /overture/places?near=47.609,-122.34,500&category=cafe
GET /overture/buildings?q=tower&format=geojson             # FeatureCollection
GET /overture/places/<gers-id>
GET /overture/search?q=Seattle                             # division geocoding
GET /overture/tiles/places/14/2624/5721.mvt                # vector tiles via ST_AsMVT
```

Resources: `places`, `buildings`, `addresses`, `divisions`, `segments`,
`connectors`, `base_features`. Collections paginate by keyset — pass
`meta.next_cursor` back as `?after=`. Limits are capped
(`config.api_max_limit`), tile responses carry public cache headers, and
everything is read-only.

MapLibre can render imported data with no separate tile server:

```js
map.addSource("places", {
  type: "vector",
  tiles: ["https://your-app.example/overture/tiles/places/{z}/{x}/{y}.mvt"]
});
```

The API is open by default; wrap it in your own auth:

```ruby
OvertureMaps.configure do |config|
  config.api_auth = ->(controller) {
    controller.head :unauthorized unless controller.request.headers["X-Api-Key"] == Rails.application.credentials.overture_api_key
  }
end
```

For public deployments, add rate limiting (e.g. Rack::Attack) in the host app.

## Keeping Data Current

Overture publishes a new release monthly. Every import records its area and
release in `overture_imported_areas`, and syncing applies the official GERS
changelog instead of a full reload — removed features are deleted, added and
changed ones are upserted:

```bash
rails overture_maps:sync:status    # which areas are behind
rails overture_maps:sync           # bring everything to the latest release
rails overture_maps:sync[2026-06-17.0]   # or a specific release
```

Areas whose release is no longer in the catalog (Overture prunes old
releases) get a full refresh automatically. Individual features can be
traced through releases with the registry:

```bash
rails overture_maps:gers:lookup[1ef5ffe6-cea9-4d4d-98f3-efbedfa4a8d7]
# first_seen, last_seen, last_changed, bbox, data file path
```

```ruby
OvertureMaps::GERS.valid_id?(id)   # dashed UUID (current) or legacy 32-hex
OvertureMaps::GERS.lookup(id)      # registry row or nil
OvertureMaps::Changelog.counts(theme: "places", type: "place", release: "2026-06-17.0")
```

## Ad-hoc Querying (no import needed)

Query Overture GeoParquet directly — DuckDB pushes the bbox filter down to
row-group statistics, so even against the remote bucket only the relevant
slice is read:

```ruby
# Count without downloading anything
OvertureMaps.query(theme: "places", bbox: [47.5, -122.4, 47.7, -122.2]).count

# Stream records (geometry parsed to RGeo features)
OvertureMaps.query(theme: "places", location: "Seattle").limit(100).each do |record|
  puts record.dig("names", "primary")
end

# Batches, GeoJSON, exports
query = OvertureMaps.query(theme: "buildings", bbox: "47.5,-122.4,47.7,-122.2")
query.each_batch(size: 500) { |batch| ... }
query.limit(1000).to_geojson                 # FeatureCollection hash
query.export("buildings.geojson")            # or .gpkg / .geojsonseq / .parquet

# Multi-type themes need an explicit type
OvertureMaps.query(theme: "transportation", type: "segment", location: "Seattle").count
```

Unlimited queries spool through the same cache files the import pipeline
uses (`config.cache_dir`), so a query warms the cache for a later import and
vice versa. Manage the cache with:

```bash
rails overture_maps:cache:list
rails overture_maps:cache:clear            # everything
rails overture_maps:cache:clear[seattle]   # matching extracts only
```

## Configuration

```ruby
# config/initializers/overture_maps.rb
OvertureMaps.configure do |config|
  config.release = "2026-06-17.0"   # pin a release (default: latest via STAC)
  config.cache_dir = "tmp/overture" # where extracts are cached
  config.batch_size = 1000          # import batch size
  config.timeout = 30               # HTTP timeout in seconds
  config.non_interactive = false    # never prompt (also OVERTURE_NON_INTERACTIVE=1)

  # Point at a mirror (e.g. MinIO on your LAN) instead of Overture's bucket:
  # config.s3_uri = "s3://my-mirror-bucket"
  # config.s3_http_url = "https://my-mirror.example.com"
end
```

## Programmatic Usage

```ruby
# Location-based import (what the rake tasks use)
runner = OvertureMaps::Import::LocationBasedRunner.new(
  theme: "places",
  location: "Seattle",              # or "47.6_-122.4_47.7_-122.2", or a BoundingBox
  model_class: OverturePlace,
  categories: ["cafe"]
).run
runner.imported_count  # => 1234

# File import with a custom transform (keyword or block)
OvertureMaps::Import.run!(
  theme: "places",
  model_class: OverturePlace,
  file_path: "/path/to/places.parquet"
) do |record|
  { id: record["id"], name: record.dig("names", "primary"), ... }
end

# Division search
OvertureMaps::Import::Downloader.search_divisions(query: "Seattle")
# => [{ id:, name:, subtype:, country:, region:, bbox:, area_km2: }, ...]

# Read a local parquet extract
reader = OvertureMaps::Import::ParquetReader.new
reader.each_record(source: "/path/to/file.parquet") { |record| ... }
```

## Model Usage

```ruby
# Spatial scopes (all models)
OverturePlace.within_bounds(47.5, -122.4, 47.7, -122.2)  # south, west, north, east
OverturePlace.near(47.6062, -122.3321, 1000)             # lat, lng, radius in meters
OverturePlace.first.to_geojson

# Places
OverturePlace.by_category("cafe")            # primary or alternate leaf category
OverturePlace.by_brand("Starbucks")
OverturePlace.by_country("US")
OverturePlace.by_operating_status("open")
OverturePlace.min_confidence(0.8)

# Buildings
OvertureBuilding.by_height(min: 50, max: 100)
OvertureBuilding.by_floors(min: 10)
OvertureBuilding.by_class("apartments")
OvertureBuilding.with_height

# Addresses
OvertureAddress.by_country("US")
OvertureAddress.by_locality("Seattle")
OvertureAddress.by_postcode("98101")
OvertureAddress.first.full_address

# Divisions
OvertureDivision.by_subtype("locality")
OvertureDivision.search_by_name("Seattle").largest_first
OvertureDivision.first.to_bounding_box

# Transportation
OvertureSegment.roads.by_class("motorway")
OvertureSegment.rails
OvertureConnector.near(47.6062, -122.3321, 500)

# Base features
OvertureBaseFeature.water
OvertureBaseFeature.land_use.by_class("park")
```

## Database Utilities

```ruby
OvertureMaps::Database.postgis_available?
OvertureMaps::Database.create_spatial_index(:overture_places)
OvertureMaps::Database.bounding_box_query(:overture_places, south, west, north, east)
OvertureMaps::Database.nearest_neighbors(:overture_places, lat, lng)
```

## MCP Server

The gem ships `overture-maps-mcp`, a read-only [MCP](https://modelcontextprotocol.io)
server over Overture's public data — no Rails app, no database. AI assistants
get tools to geocode place names, query and count features, export GeoJSON,
and look up GERS ids, all streaming from the bucket with bbox pushdown.

```bash
gem install overture_maps mcp
```

Claude Desktop config:

```json
{
  "mcpServers": {
    "overture-maps": { "command": "overture-maps-mcp" }
  }
}
```

Tools: `geocode`, `query_features`, `count_features`, `export_geojson`,
`gers_lookup`, `list_releases`. The server is read-only and never touches
your application database.

## Attribution

Overture data carries per-theme licenses (ODbL for OSM-derived themes, CDLA-Permissive-2.0 for places, per-source terms for addresses). The imported `sources` column preserves which upstream datasets contributed, and the gem turns that into the notices your app needs:

```ruby
OvertureMaps::Attribution.notices
# => ["Overture Maps Foundation — overturemaps.org",
#     "© OpenStreetMap contributors (ODbL)",
#     "Data sources: Microsoft, meta"]
OvertureMaps::Attribution.text   # one line for a map corner
```

Map UIs can fetch the same via `GET /overture/attribution`. See [docs.overturemaps.org/attribution](https://docs.overturemaps.org/attribution/) for the authoritative requirements.

## Requirements

- Ruby >= 3.0
- Rails >= 7.0
- PostgreSQL with PostGIS
- DuckDB (the `duckdb` gem, a `duckdb` binary on PATH, or automatic CLI download)

## Development

```bash
bundle install
bundle exec rspec
```

## License

MIT License — see LICENSE.txt

## Links

- [Overture Maps](https://overturemaps.org/)
- [Overture Maps Documentation](https://docs.overturemaps.org/)
