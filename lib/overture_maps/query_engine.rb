# frozen_string_literal: true

require "open3"
require "json"
require "fileutils"

module OvertureMaps
  # Runs spatial SQL against Overture GeoParquet via DuckDB.
  #
  # Prefers the ruby-duckdb gem (in-process, bound parameters, no shell).
  # Falls back to the DuckDB CLI: PATH lookup first, then a verified download.
  # All user-supplied values go through bound parameters (native) or strict
  # literal quoting (CLI) — never raw string interpolation.
  class QueryEngine
    DUCKDB_CLI_VERSION = "1.5.3"

    class Error < OvertureMaps::Error; end

    class << self
      def instance
        @instance ||= new
      end

      def reset!
        @instance = nil
      end
    end

    # Returns an array of row hashes. Intended for modest result sets
    # (division searches, lookups) — bulk data must use #copy_to.
    def query(sql, params = [])
      if native?
        native_query(sql, params)
      else
        cli_query(sql, params)
      end
    end

    # Exports a query's results to a local file (parquet by default). This is
    # the bulk-data path: DuckDB streams row groups from S3 and writes the
    # file without materializing results in Ruby.
    def copy_to(sql, params: [], output_path:, format: "parquet")
      copy_sql = build_copy_sql(sql, output_path, format)

      if native?
        native_execute(copy_sql, params)
      else
        cli_execute(interpolate(copy_sql, params))
      end
      output_path
    end

    def native?
      return @native unless @native.nil?

      @native =
        begin
          require "duckdb"
          true
        rescue LoadError
          false
        end
    end

    private

    def build_copy_sql(sql, output_path, format)
      path = quote_string(output_path.to_s)
      case format.to_s.downcase
      when "parquet"
        "COPY (#{sql}) TO #{path} (FORMAT PARQUET)"
      when "geojson"
        "COPY (#{sql}) TO #{path} WITH (FORMAT GDAL, DRIVER 'GeoJSON')"
      when "geojsonseq"
        "COPY (#{sql}) TO #{path} WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq')"
      when "gpkg", "geopackage"
        "COPY (#{sql}) TO #{path} WITH (FORMAT GDAL, DRIVER 'GPKG')"
      else
        raise ArgumentError, "unknown export format: #{format}"
      end
    end

    def init_statements
      region = OvertureMaps.configuration.s3_region
      raise Error, "invalid s3_region: #{region.inspect}" unless region.match?(/\A[a-z0-9-]+\z/)

      # Division areas and building footprints have large row groups; the
      # httpfs default of 30s times out on slower links, so give bulk
      # fetches at least two minutes and a few retries.
      http_timeout_ms = [Integer(OvertureMaps.configuration.timeout) * 1000, 120_000].max

      [
        "INSTALL spatial",
        "LOAD spatial",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET s3_region='#{region}'",
        "SET http_timeout=#{http_timeout_ms}",
        "SET http_retries=5"
      ]
    end

    # --- native (ruby-duckdb) backend ---

    def native_connection
      @native_db ||= ::DuckDB::Database.open
      con = @native_db.connect
      init_statements.each { |stmt| con.query(stmt) }
      con
    end

    def native_query(sql, params)
      con = native_connection
      result = con.query(sql, *params)
      columns = result.columns.map(&:name)
      result.map { |row| columns.zip(row.map { |v| normalize_value(v) }).to_h }
    ensure
      con&.close
    end

    def native_execute(sql, params)
      con = native_connection
      con.query(sql, *params)
      nil
    ensure
      con&.close
    end

    def normalize_value(value)
      case value
      when ::DuckDB::Interval then value.to_s
      else value
      end
    rescue NameError
      value
    end

    # --- CLI backend ---

    def cli_query(sql, params)
      output = run_cli(interpolate(sql, params), json: true)
      output = output.strip
      return [] if output.empty?

      JSON.parse(output)
    end

    def cli_execute(sql)
      run_cli(sql, json: false)
      nil
    end

    def run_cli(sql, json:)
      cli = self.class.cli_path
      full_sql = (init_statements + [sql]).join(";\n") + ";"
      argv = [cli]
      argv << "-json" if json
      argv += ["-batch", "-noheader"] unless json

      stdout, stderr, status = Open3.capture3(*argv, stdin_data: full_sql)
      unless status.success?
        detail = stderr.strip.empty? ? stdout.strip : stderr.strip
        raise Error, "DuckDB CLI failed (exit #{status.exitstatus}): #{detail}"
      end

      stdout
    end

    # Replaces `?` placeholders with safely quoted literals for the CLI
    # backend. Question marks inside string literals are not supported in
    # our SQL, which we control.
    def interpolate(sql, params)
      remaining = params.dup
      result = sql.gsub("?") do
        raise Error, "not enough bind params for SQL" if remaining.empty?

        quote(remaining.shift)
      end
      raise Error, "too many bind params for SQL" unless remaining.empty?

      result
    end

    def quote(value)
      case value
      when Integer, Float then value.to_s
      when Numeric then Float(value).to_s
      when nil then "NULL"
      when true, false then value.to_s
      else quote_string(value.to_s)
      end
    end

    def quote_string(str)
      "'#{str.gsub("'", "''")}'"
    end

    class << self
      # Resolve the DuckDB CLI: configured path, PATH, cached download —
      # downloading a pinned release if necessary.
      def cli_path
        configured = OvertureMaps.configuration.duckdb_cli_path
        return configured if configured && File.executable?(configured)

        found = find_in_path("duckdb")
        return found if found

        cached = cached_cli_path
        return cached if File.executable?(cached)

        download_cli(cached)
        cached
      end

      def cli_available?
        !!(OvertureMaps.configuration.duckdb_cli_path || find_in_path("duckdb") ||
           File.executable?(cached_cli_path))
      end

      private

      def find_in_path(name)
        ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).each do |dir|
          candidate = File.join(dir, name)
          return candidate if File.file?(candidate) && File.executable?(candidate)
        end
        nil
      end

      def cached_cli_path
        File.join(cache_root, "duckdb-#{DUCKDB_CLI_VERSION}", "duckdb")
      end

      def cache_root
        base = ENV["XDG_CACHE_HOME"] || File.join(Dir.home, ".cache")
        File.join(base, "overture_maps")
      end

      def cli_archive_name
        case RUBY_PLATFORM
        when /darwin/ then "duckdb_cli-osx-universal.zip"
        when /aarch64-linux|arm64-linux/ then "duckdb_cli-linux-arm64.zip"
        when /linux/ then "duckdb_cli-linux-amd64.zip"
        else
          raise Error, "no DuckDB CLI build for #{RUBY_PLATFORM}; install duckdb " \
                       "manually (https://duckdb.org) or add the duckdb gem"
        end
      end

      def download_cli(target)
        require "overture_maps/storage"

        url = "https://github.com/duckdb/duckdb/releases/download/v#{DUCKDB_CLI_VERSION}/#{cli_archive_name}"
        dir = File.dirname(target)
        FileUtils.mkdir_p(dir)
        zip_path = File.join(dir, "cli.zip")

        OvertureMaps.configuration.logger&.info("Downloading DuckDB CLI #{DUCKDB_CLI_VERSION}...")
        Storage.download_url(url, to: zip_path)

        system("unzip", "-o", "-q", "-j", zip_path, "-d", dir, exception: true)
        FileUtils.chmod("+x", target)
      ensure
        FileUtils.rm_f(zip_path) if zip_path
      end
    end
  end
end
