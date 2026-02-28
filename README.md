# Overture Maps Ruby Gem

A Ruby gem for integrating with [Overture Maps](https://overturemaps.org/) - an open map data foundation providing geospatial data. Import Parquet data files into your Rails application.

## Features

- **Data Import**: Import Parquet data files into your PostgreSQL/PostGIS database
- **Rails Integration**: Model generators, rake tasks, and PostGIS utilities
- **RGeo Support**: Full geometry support for geospatial queries

## Installation

Add to your Rails application's Gemfile:

```ruby
gem "overture_maps"
```

Then run:

```bash
bundle install
```

For Parquet file support, also install the parquet gem:

```bash
gem install parquet
```

For S3 cloud storage support (optional):

```bash
gem install aws-sdk-s3
```

## Database Setup

This gem requires PostgreSQL with PostGIS extension.

Create a new Rails application with PostGIS:

```bash
rails new my_app -d postgresql
```

Enable the PostGIS extension:

```ruby
# db/migrate/xxxx_enable_postgis.rb
class EnablePostgis < ActiveRecord::Migration[8.0]
  def change
    enable_extension "postgis"
  end
end
```

## Model Generators

Generate Rails models and migrations for Overture data:

### Install All Models

```bash
rails generate overture_maps:install
```

This creates:
- Migration for `overture_places` table
- Migration for `overture_buildings` table
- Migration for `overture_addresses` table
- Model files

### Generate Individual Models

```bash
# Generate Place model
rails generate overture_maps:place

# Generate Building model
rails generate overture_maps:building

# Generate Address model
rails generate overture_maps:address
```

### Run Migrations

```bash
rails db:migrate
```

## Data Import

### Download Parquet Files

Download Parquet files directly from S3 or Azure without needing to use the AWS/Azure CLI:

```bash
# Download all places data (global)
rails overture_maps:download:places

# Download places (specific type - default is "place")
rails overture_maps:download:places[place]

# Download a specific version
rails overture_maps:download:places[place,2025-01-17]

# Download to custom directory
rails overture_maps:download:places[place,,/custom/path]

# Download buildings (includes "building" and "building_part" types)
rails overture_maps:download:buildings
rails overture_maps:download:buildings[building]
rails overture_maps:download:buildings[building_part]

# Download addresses
rails overture_maps:download:addresses

# Download base data (land, water, land_use, etc.)
rails overture_maps:download:base
rails overture_maps:download:base[land]

# Download divisions (countries, states, etc.)
rails overture_maps:download:divisions

# Download transportation (roads, etc.)
rails overture_maps:download:transportation

# Download all themes
rails overture_maps:download:all

# Download from Azure instead of S3
rails overture_maps:download:azure:places
```

**Options:**
- `type` - Feature type within the theme (e.g., `place`, `building`, `land`). Defaults to first type in theme.
- `version` - Data version (e.g., `2025-01-17`). Defaults to latest available.
- `output_dir` - Directory to save files. Defaults to `tmp/overture`.

### List Available Data

```bash
# List available themes and their types
rails overture_maps:download:themes

# List available versions
rails overture_maps:download:versions

# List types available for a theme
rails overture_maps:download:types[buildings]

# List files without downloading (shows S3/Azure paths)
rails overture_maps:download:list[places]
rails overture_maps:download:list[buildings,building]
```

### Download by Geographic Area

Download data for a specific area using DuckDB for server-side filtering (much faster than downloading all files):

```bash
# Download places within a bounding box
# Arguments: theme, lat1, lng1, lat2, lng2
# lat1,lng1 = top-left corner, lat2,lng2 = bottom-right corner
rails overture_maps:download:bbox[places,49.5,-125,47,-121]

# Download buildings within bounding box
rails overture_maps:download:bbox[buildings,40.6,-74.1,40.8,-73.9]

# Download near a center point (lat, lng, radius in meters)
rails overture_maps:download:nearby[places,40.7128,-74.006,10000]

# Download with custom type, version, and output directory
rails overture_maps:download:bbox[buildings,40.6,-74.1,40.8,-73.9,building,2025-01-17,/custom/path]
```

**Note:** The bbox and nearby tasks use DuckDB to query S3 directly with spatial filtering, which is much more efficient than downloading all files. Requires the `duckdb` gem.

### Download by Division Name

Download data for a specific geographic division (country, state, county, city, etc.):

```bash
# Search for divisions by name
rails overture_maps:download:search_divisions[California]

# Download places for a division
rails overture_maps:download:division[places,California]

# Download buildings for a division
rails overture_maps:download:division[buildings,Washington]

# With custom options
rails overture_maps:download:division[places,King\ County,building,2025-01-17,/custom/path]
```

The task will:
1. Search for divisions matching the name
2. Show a list if multiple matches are found
3. Ask you to select the correct one
4. Extract the bounding box from the division's shape
5. Download the requested data within that bounding box

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

### Import from Parquet Files

```bash
# Import places
rails overture_maps:import:places[/path/to/places.parquet]

# Import buildings
rails overture_maps:import:buildings[/path/to/buildings.parquet]

# Import addresses
rails overture_maps:import:addresses[/path/to/addresses.parquet]

# Check record count
rails overture_maps:count[/path/to/file.parquet]
```

### Programmatic Import

```ruby
# Import with custom transform
OvertureMaps::Import.run!(
  theme: "places",
  model_class: OverturePlace,
  file_path: "/path/to/places.parquet",
  batch_size: 1000
) do |record|
  # Transform record before import
  {
    id: record["id"],
    names: record["names"],
    categories: record["categories"]&.to_json,
    geometry: parse_geometry(record["geometry"]),
    country: record["country"],
    created_at: Time.current,
    updated_at: Time.current
  }
end
```

### Read Parquet Files Directly

```ruby
reader = OvertureMaps::Import::ParquetReader.new(
  theme: "places",
  region: "us-east-1"
)

reader.each_record(source: "/path/to/file.parquet") do |record|
  puts record["id"]
end
```

## Model Usage

### Querying Places

```ruby
# All places
OverturePlace.all

# Within bounding box
OverturePlace.within_bounds(40.7, -74.1, 40.9, -73.9)

# Near a point (within 1km)
OverturePlace.near(40.7128, -74.006, 1000)

# By category
OverturePlace.by_category("cafes")

# By country
OvertartPlace.by_country("US")

# By brand
OverturePlace.by_brand("Starbucks")

# Convert to GeoJSON
place = OverturePlace.first
place.to_geojson
```

### Querying Buildings

```ruby
# All buildings
OvertureBuilding.all

# By height range
OvertureBuilding.by_height(min: 50, max: 100)

# By level range
OvertureBuilding.by_level(min: 10)

# Buildings with height data
OvertureBuilding.with_height
```

### Querying Addresses

```ruby
# All addresses
OvertureAddress.all

# By country
OvertureAddress.by_country("US")

# By locality
OvertureAddress.by_locality("San Francisco")

# By postcode
OvertureAddress.by_postcode("94102")
```

## Database Utilities

PostGIS helper methods:

```ruby
# Check if PostGIS is available
OvertureMaps::Database.postgis_available?

# Create spatial index
OvertureMaps::Database.create_spatial_index(:overture_places)

# Bounding box query
OvertureMaps::Database.bounding_box_query(
  :overture_places,
  south, west, north, east
)

# Nearest neighbors
OvertureMaps::Database.nearest_neighbors(
  :overture_places,
  lat, lng,
  limit: 10
)
```

## Model Scopes

| Model | Scope | Description |
|-------|-------|-------------|
| `OverturePlace` | `within_bounds(s, w, n, e)` | Places within bounding box |
| `OverturePlace` | `near(lat, lng, radius)` | Places near a point |
| `OverturePlace` | `by_category(categories)` | Filter by category |
| `OverturePlace` | `by_country(country)` | Filter by country |
| `OverturePlace` | `by_brand(brand)` | Filter by brand |
| `OvertureBuilding` | `by_height(min:, max:)` | Filter by height |
| `OvertureBuilding` | `by_level(min:, max:)` | Filter by level |
| `OvertureBuilding` | `with_height` | Only buildings with height |
| `OvertureAddress` | `by_country(country)` | Filter by country |
| `OvertureAddress` | `by_locality(locality)` | Filter by city |
| `OvertureAddress` | `by_region(region)` | Filter by region |
| `OvertureAddress` | `by_postcode(postcode)` | Filter by postcode |

## Requirements

- Ruby >= 3.0
- Rails >= 7.0
- PostgreSQL with PostGIS extension
- RGeo (geospatial, included)
- Parquet gem (for Parquet file support)

## Development

Run tests:

```bash
bundle exec rspec
```

## License

MIT License - see LICENSE.txt

## Links

- [Overture Maps](https://overturemaps.org/)
- [Overture Maps Documentation](https://docs.overturemaps.org/)
