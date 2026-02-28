module OvertureMaps
  class Configuration
    attr_accessor :api_key, :base_url, :adapter, :timeout

    def initialize
      @api_key = nil
      @base_url = "https://api.overturemapsapi.com"
      @adapter = Faraday.default_adapter
      @timeout = 30
    end

    def validate!
      raise ConfigurationError, "api_key is required" if api_key.nil? || api_key.empty?
    end
  end
end
