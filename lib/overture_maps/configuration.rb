# frozen_string_literal: true

module OvertureMaps
  class Configuration
    DEFAULT_S3_HTTP_URL = "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com"
    DEFAULT_S3_URI = "s3://overturemaps-us-west-2"
    DEFAULT_S3_REGION = "us-west-2"

    # release: Overture release string (e.g. "2026-06-17.0"). nil means "latest available".
    # cache_dir: where downloaded parquet extracts are stored.
    # s3_http_url/s3_uri/s3_region: data source; point at a mirror (e.g. MinIO) to avoid
    #   hitting Overture's bucket from every host.
    # batch_size: default import batch size.
    # timeout: HTTP open/read timeout in seconds.
    # non_interactive: never prompt; pick the best division match and prefer fresh downloads.
    # duckdb_cli_path: explicit path to the duckdb binary (otherwise PATH, then auto-download).
    # logger: receives progress output; defaults to stdout logging from the rake layer.
    # api_auth: callable invoked with the controller before every API
    #   request; render/head inside it to reject (nil = open access).
    # api_default_limit / api_max_limit: collection endpoint page sizing.
    # tile_feature_limit: per-tile feature cap for the MVT endpoint.
    attr_accessor :release, :cache_dir, :s3_http_url, :s3_uri, :s3_region,
                  :batch_size, :timeout, :non_interactive, :duckdb_cli_path, :logger,
                  :api_auth, :api_default_limit, :api_max_limit, :tile_feature_limit

    def initialize
      @release = nil
      @cache_dir = "tmp/overture"
      @s3_http_url = DEFAULT_S3_HTTP_URL
      @s3_uri = DEFAULT_S3_URI
      @s3_region = DEFAULT_S3_REGION
      @batch_size = 1000
      @timeout = 30
      @non_interactive = !ENV["OVERTURE_NON_INTERACTIVE"].to_s.empty?
      @duckdb_cli_path = ENV["OVERTURE_DUCKDB_CLI"]
      @logger = nil
      @api_auth = nil
      @api_default_limit = 100
      @api_max_limit = 1000
      @tile_feature_limit = 20_000
    end
  end
end
