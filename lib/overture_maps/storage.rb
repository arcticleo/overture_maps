# frozen_string_literal: true

require "net/http"
require "uri"
require "rexml/document"
require "fileutils"

module OvertureMaps
  # Anonymous HTTP access to the public Overture bucket (or a configured
  # mirror): object listing via the S3 REST API's XML responses and streaming
  # downloads. No AWS SDK, no credentials, no global state.
  module Storage
    class Error < OvertureMaps::Error; end

    MAX_REDIRECTS = 5

    class << self
      # Lists objects under a prefix. Returns { objects: [{key:, size:}],
      # prefixes: [String] }, following continuation tokens.
      def list(prefix:, delimiter: nil)
        objects = []
        prefixes = []
        token = nil

        loop do
          params = { "list-type" => "2", "prefix" => prefix }
          params["delimiter"] = delimiter if delimiter
          params["continuation-token"] = token if token

          doc = REXML::Document.new(get("#{base_url}/?#{URI.encode_www_form(params)}"))
          root = doc.root
          raise Error, "unexpected listing response" unless root&.name == "ListBucketResult"

          root.each_element("Contents") do |el|
            objects << {
              key: el.elements["Key"]&.text,
              size: el.elements["Size"]&.text.to_i
            }
          end
          root.each_element("CommonPrefixes/Prefix") { |el| prefixes << el.text }

          token = root.elements["NextContinuationToken"]&.text
          break unless token
        end

        { objects: objects, prefixes: prefixes }
      end

      # Streams an object to a local file. Skips the download when the local
      # file already has the expected size.
      def download(key, to:, expected_size: nil)
        if expected_size && File.exist?(to) && File.size(to) == expected_size
          return :skipped
        end

        download_url("#{base_url}/#{escape_key(key)}", to: to)
        :downloaded
      end

      # Downloads any URL to a file, following redirects, writing through a
      # temp file so failures never leave a truncated file at the target path.
      def download_url(url, to:)
        FileUtils.mkdir_p(File.dirname(to))
        tmp = "#{to}.part"

        fetch_streaming(url) do |response|
          File.open(tmp, "wb") do |file|
            response.read_body { |chunk| file.write(chunk) }
          end
        end

        FileUtils.mv(tmp, to)
        to
      ensure
        FileUtils.rm_f(tmp) if tmp && File.exist?(tmp)
      end

      # GET returning the body as a string, following redirects.
      def get(url)
        body = nil
        fetch_streaming(url) { |response| body = response.body }
        body
      end

      private

      def base_url
        OvertureMaps.configuration.s3_http_url.chomp("/")
      end

      def escape_key(key)
        key.split("/").map { |part| URI.encode_uri_component(part) }.join("/")
      end

      def fetch_streaming(url, redirects_left = MAX_REDIRECTS, &block)
        uri = URI(url)
        timeout = OvertureMaps.configuration.timeout

        Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https",
                        open_timeout: timeout, read_timeout: timeout) do |http|
          http.request(Net::HTTP::Get.new(uri)) do |response|
            case response
            when Net::HTTPRedirection
              raise Error, "too many redirects for #{url}" if redirects_left.zero?

              location = response["location"]
              raise Error, "redirect without location from #{url}" unless location

              response.read_body # drain before following
              return fetch_streaming(URI.join(url, location).to_s, redirects_left - 1, &block)
            when Net::HTTPSuccess
              yield response
            else
              raise Error, "HTTP #{response.code} for #{url}"
            end
          end
        end
      end
    end
  end
end
