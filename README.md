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

### Build Dependencies

The parquet and aws-sdk-s3 gems require native extensions:

```bash
sudo apt-get install -y cmake build-essential
bundle install
```

The gem automatically downloads the DuckDB CLI binary when needed for bbox/division filtering.

## Getting Started

### Install the Gem

Add to your Gemfile and run `bundle install`.

### Generate Models and Migrations

```bash
rails generate overture_maps:install
```

This creates:
- Migration to enable the PostGIS extension
- Migration for `overture_places` table
- Migration for `overture_buildings` table
- Migration for `overture_addresses` table
- Migration for `overture_categories` table
- Model files

Then run migrations:

```bash
rails db:migrate
```

### Fetch Categories (Recommended)

Before importing places, fetch the categories taxonomy:

```bash
rails overture_maps:categories:populate
```

This fetches all ~2,100 categories from Overture Maps schema. You can then list them:

```bash
# List all categories
rails overture_maps:categories:list

# List just primary categories
rails overture_maps:categories:primary
```

### Generate Individual Models

```bash
# Generate Place model
rails generate overture_maps:place

# Generate Building model
rails generate overture_maps:building

# Generate Address model
rails generate overture_maps:address
```

## Importing Data

Import Overture Maps data directly into your Rails application's database.

### Quick Start

Import data for any city, state, or country by name:

```bash
rails overture_maps:import:places[Seattle]
```

That's it! The gem will:
1. Search for divisions matching "Seattle"
2. Prompt you to select if multiple matches exist
3. Stream data directly from S3 into your database with spatial filtering

### Import by Location Name

```bash
# Import places for a city, state, or country
rails overture_maps:import:places[Seattle]
rails overture_maps:import:places[California]
rails overture_maps:import:places[Germany]

# For names with spaces, use quotes or escape the space
rails "overture_maps:import:places[New York]"
rails overture_maps:import:places[New\ York]
```

### Import by Bounding Box

```bash
# Import within a geographic bounding box (lat1,lng1,lat2,lng2)
rails overture_maps:import:places[47.606,-122.336,47.609,-122.333]
rails overture_maps:import:buildings[47.606,-122.336,47.609,-122.333]
```

### Import with Category Filtering

```bash
# Import only food & drink establishments
rails overture_maps:import:places[Seattle,eat_and_drink]

# Import multiple categories
rails overture_maps:import:places[Seattle,"eat_and_drink,shopping"]

# See available primary categories
rails overture_maps:categories:primary
```

### Import Buildings and Addresses

```bash
rails overture_maps:import:buildings[Seattle]
rails overture_maps:import:addresses[Seattle]
```

### Import All Themes

```bash
# Import all themes for a location
rails overture_maps:import:all[Seattle]

# Import all themes using bounding box (use underscores)
rails overture_maps:import:all[47.606_-122.336_47.609_-122.333]
```

### How Import Works

When you run an import task:

1. **Search**: The gem searches for geographic divisions matching your location name
2. **Select**: If multiple matches are found, you'll be prompted to select the correct one
3. **Local File Check**: If a previously downloaded file exists for this location, you'll be asked:
   - `y` - Import from local file (faster)
   - `n` - Cancel
   - `download` - Download fresh data from S3 (may be newer)
4. **S3 Import**: If no local file exists, data is streamed directly from S3 with spatial filtering using DuckDB

### Search Divisions First

Not sure of the exact name? Search first:

```bash
rails overture_maps:import:search[Seattle]

# Then import using the exact name from the results
rails overture_maps:import:places[Seattle]
```

### Check Import Statistics

```bash
rails overture_maps:import:stats
```

### List Available Versions

```bash
rails overture_maps:import:versions
```

---

## Downloading Data (Optional)

**Download is optional.** Import tasks stream data directly from S3 by default. Use download tasks when you want to:

- Keep raw Parquet files locally for offline use
- Re-import the same data multiple times without re-downloading
- Work with the data outside of Rails (e.g., in QGIS, pandas, etc.)

### Download by Location Name

```bash
# Download places for a city
rails overture_maps:download:places[Seattle]

# Download buildings for a geographic area
rails overture_maps:download:buildings[California]

# Download addresses for a country
rails overture_maps:download:addresses[Germany]

# For names with spaces
rails "overture_maps:download:places[New York]"
```

### Download by Bounding Box

```bash
# Download places within a bounding box
rails overture_maps:download:bbox[places,49.5,-125,47,-121]

# Download near a center point (lat, lng, radius in meters)
rails overture_maps:download:nearby[places,40.7128,-74.006,10000]
```

### Download Complete Theme Files

```bash
# Download all places data files (global - large!)
rails overture_maps:download:places

# Download specific theme files
rails overture_maps:download:buildings
rails overture_maps:download:addresses
rails overture_maps:download:transportation

# Download from Azure instead of S3
rails overture_maps:download:azure:places
```

### Using Downloaded Files

Once you've downloaded data, the import task will automatically detect it:

```bash
# Download once
rails overture_maps:download:places[Seattle]

# Import will prompt to use the local file
rails overture_maps:import:places[Seattle]
# → Found local file: tmp/overture/places_seattle.parquet (15.8 MB)
# → Import from this file? (y/n/download)
```

### Download Options

**Options:**
- `type` - Feature type within the theme (e.g., `place`, `building`, `land`)
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

# List files without downloading
rails overture_maps:download:list[places]
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

---

## Advanced Usage

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
    categories: record["categories"],
    geometry: parse_geometry(record["geometry"]),
    country: record["country"],
    created_at: Time.current,
    updated_at: Time.current
  }
end
```

### Read Parquet Files Directly

```ruby
reader = OvertureMaps::Import::ParquetReader.new(theme: "places")

reader.each_record(source: "/path/to/file.parquet") do |record|
  puts record["id"]
end
```

---

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
OverturePlace.by_country("US")

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
