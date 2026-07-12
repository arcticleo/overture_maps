# frozen_string_literal: true

require "overture_maps"

begin
  require "mcp"
  require "mcp/server/transports/stdio_transport"
rescue LoadError
  abort "The MCP server needs the official mcp gem: gem install mcp"
end

require "json"

module OvertureMaps
  # A read-only MCP (Model Context Protocol) server over Overture's public
  # GeoParquet — no Rails app, no database. Queries stream through DuckDB
  # with bbox pushdown, so nothing is bulk-downloaded.
  #
  #   overture-maps-mcp    # speaks MCP over stdio
  #
  # Claude Desktop config:
  #   { "mcpServers": { "overture-maps": { "command": "overture-maps-mcp" } } }
  module MCPServer
    MAX_FEATURES = 100

    module Helpers
      module_function

      def text_response(payload)
        MCP::Tool::Response.new([{ type: "text", text: JSON.pretty_generate(payload) }])
      end

      def error_response(message)
        MCP::Tool::Response.new([{ type: "text", text: JSON.generate(error: message) }], error: true)
      end

      # Accepts either a named-corner bbox or a location name.
      def resolve_bbox(args)
        if args[:west] && args[:south] && args[:east] && args[:north]
          BoundingBox.new(lat1: args[:south], lng1: args[:west],
                          lat2: args[:north], lng2: args[:east])
        elsif args[:location]
          results = DivisionSearch.search(query: args[:location])
          raise Error, "no divisions found matching #{args[:location].inspect}" if results.empty?

          results.first[:bbox]
        else
          raise ArgumentError, "provide west/south/east/north or a location name"
        end
      end

      def bbox_properties
        {
          location: { type: "string", description: "Place name to geocode (e.g. 'Seattle'); alternative to the bbox corners" },
          west: { type: "number" }, south: { type: "number" },
          east: { type: "number" }, north: { type: "number" }
        }
      end
    end

    class GeocodeTool < MCP::Tool
      tool_name "geocode"
      description "Find geographic divisions (countries, regions, cities, neighborhoods) by name. " \
                  "Returns each match with its bounding box [west, south, east, north]."
      input_schema(
        properties: { query: { type: "string", description: "Division name, e.g. 'Seattle'" } },
        required: ["query"]
      )

      def self.call(query:, server_context: nil)
        results = DivisionSearch.search(query: query).map do |r|
          {
            name: r[:name], subtype: r[:subtype], country: r[:country], region: r[:region],
            area_km2: r[:area_km2],
            bbox: [r[:bbox].min_lng, r[:bbox].min_lat, r[:bbox].max_lng, r[:bbox].max_lat]
          }
        end
        Helpers.text_response(results)
      rescue OvertureMaps::Error, ArgumentError => e
        Helpers.error_response(e.message)
      end
    end

    class QueryFeaturesTool < MCP::Tool
      tool_name "query_features"
      description "Fetch Overture Maps features (places, buildings, addresses, roads, ...) in an " \
                  "area as GeoJSON features. Themes: places, buildings, addresses, divisions, " \
                  "transportation (type segment/connector), base (type water/land/land_use/...). " \
                  "Limit is capped at #{MAX_FEATURES}."
      input_schema(
        properties: Helpers.bbox_properties.merge(
          theme: { type: "string", description: "Overture theme, e.g. 'places'" },
          type: { type: "string", description: "Feature type for multi-type themes, e.g. 'segment'" },
          category: { type: "string", description: "Places only: comma-separated leaf categories, e.g. 'cafe,coffee_shop'" },
          limit: { type: "integer", description: "Max features (default 20, cap #{MAX_FEATURES})" }
        ),
        required: ["theme"]
      )

      def self.call(theme:, type: nil, category: nil, limit: nil, server_context: nil, **bbox_args)
        bbox = Helpers.resolve_bbox(bbox_args)
        limit = [(limit || 20).to_i.clamp(1, MAX_FEATURES), MAX_FEATURES].min
        wanted = category&.split(",")&.map(&:strip)

        features = []
        Query.new(theme: theme, type: type, bbox: bbox, limit: limit * 5).each do |record|
          if wanted
            cats = record["categories"]
            primary = cats.is_a?(Hash) ? cats["primary"] : record["basic_category"]
            next unless wanted.include?(primary) ||
                        (cats.is_a?(Hash) && (Array(cats["alternate"]) & wanted).any?)
          end

          geometry = record.delete("geometry")
          properties = record.except("bbox", "names", "sources")
          properties["name"] = record.dig("names", "primary")
          features << {
            type: "Feature",
            geometry: geometry && RGeo::GeoJSON.encode(geometry),
            properties: properties
          }
          break if features.length >= limit
        end

        Helpers.text_response({ type: "FeatureCollection", features: features })
      rescue OvertureMaps::Error, ArgumentError => e
        Helpers.error_response(e.message)
      end
    end

    class CountFeaturesTool < MCP::Tool
      tool_name "count_features"
      description "Count Overture Maps features of a theme in an area without downloading them."
      input_schema(
        properties: Helpers.bbox_properties.merge(
          theme: { type: "string" },
          type: { type: "string" }
        ),
        required: ["theme"]
      )

      def self.call(theme:, type: nil, server_context: nil, **bbox_args)
        bbox = Helpers.resolve_bbox(bbox_args)
        count = Query.new(theme: theme, type: type, bbox: bbox).count
        Helpers.text_response({ theme: theme, type: type, bbox: bbox.to_s, count: count })
      rescue OvertureMaps::Error, ArgumentError => e
        Helpers.error_response(e.message)
      end
    end

    class ExportGeojsonTool < MCP::Tool
      tool_name "export_geojson"
      description "Export Overture Maps features for an area to a local file. Format comes from " \
                  "the extension: .geojson, .geojsonseq, .gpkg, or .parquet."
      input_schema(
        properties: Helpers.bbox_properties.merge(
          theme: { type: "string" },
          type: { type: "string" },
          path: { type: "string", description: "Output file path" },
          limit: { type: "integer" }
        ),
        required: %w[theme path]
      )

      def self.call(theme:, path:, type: nil, limit: nil, server_context: nil, **bbox_args)
        bbox = Helpers.resolve_bbox(bbox_args)
        query = Query.new(theme: theme, type: type, bbox: bbox)
        query = query.limit(limit) if limit
        output = query.export(File.expand_path(path))
        Helpers.text_response({ exported: output, bytes: File.size(output) })
      rescue OvertureMaps::Error, ArgumentError => e
        Helpers.error_response(e.message)
      end
    end

    class GersLookupTool < MCP::Tool
      tool_name "gers_lookup"
      description "Look up a GERS id (Overture's stable entity id) in the official registry: " \
                  "when it first appeared, when it last changed, and where it lives."
      input_schema(
        properties: { id: { type: "string", description: "GERS id (dashed UUID)" } },
        required: ["id"]
      )

      def self.call(id:, server_context: nil)
        row = GERS.lookup(id)
        Helpers.text_response(row || { error: "not found in the registry" })
      rescue OvertureMaps::Error, ArgumentError => e
        Helpers.error_response(e.message)
      end
    end

    class ListReleasesTool < MCP::Tool
      tool_name "list_releases"
      description "List available Overture Maps data releases, newest first."
      input_schema(properties: {}, required: [])

      def self.call(server_context: nil)
        Helpers.text_response({ releases: Releases.all, latest: Releases.latest })
      rescue OvertureMaps::Error => e
        Helpers.error_response(e.message)
      end
    end

    TOOLS = [
      GeocodeTool, QueryFeaturesTool, CountFeaturesTool,
      ExportGeojsonTool, GersLookupTool, ListReleasesTool
    ].freeze

    def self.start
      server = MCP::Server.new(
        name: "overture-maps",
        version: OvertureMaps::VERSION,
        instructions: "Query Overture Maps open geospatial data: geocode place names, fetch and " \
                      "count features (places, buildings, roads, ...), export GeoJSON, and look " \
                      "up GERS ids. Read-only; data streams from Overture's public bucket.",
        tools: TOOLS
      )
      MCP::Server::Transports::StdioTransport.new(server).open
    end
  end
end
