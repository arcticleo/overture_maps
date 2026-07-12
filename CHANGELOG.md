# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - Unreleased

Initial release.

### Added

- Location-based imports for all six Overture themes (places, buildings,
  addresses, divisions, transportation, base) into PostGIS: bbox-filtered
  GeoParquet extracts via DuckDB with row-group pushdown, idempotent
  `upsert_all` imports keyed on GERS ids, and shared extract caching.
- Division geocoding (`rails overture_maps:import:places[Seattle]`) with
  local-first resolution once divisions are imported.
- Ad-hoc query API (`OvertureMaps.query`) over remote or cached GeoParquet —
  counts, streaming, GeoJSON/GPKG exports — with no database required.
- Release discovery via Overture's STAC catalog; changelog-driven syncing
  (`rails overture_maps:sync`) that applies removals and upserts instead of
  full reloads; GERS registry lookups.
- Category taxonomy with group expansion (`eat_and_drink` → leaf categories).
- Mountable REST engine: JSON/GeoJSON feature endpoints with keyset
  pagination, division search, license-correct attribution notices, and
  Mapbox Vector Tiles straight from PostGIS (`ST_AsMVT`).
- `overture-maps-mcp`: a read-only MCP server over the public Overture
  bucket (geocode, query/count features, GeoJSON export, GERS lookup).
- Install generator creating migrations, models, and the engine mount.
