# Configure OvertureMaps API key
#
# Example usage:
#   OvertureMaps.configure do |config|
#     config.api_key = ENV["OVERTOURE_MAPS_API_KEY"]
#   end
#
# Or set directly:
#   OvertureMaps.api_key = ENV["OVERTOURE_MAPS_API_KEY"]

OvertureMaps.configure do |config|
  config.api_key = ENV["OVERTOURE_MAPS_API_KEY"]
  config.base_url = ENV["OVERTOURE_MAPS_BASE_URL"] if ENV["OVERTOURE_MAPS_BASE_URL"]
end
