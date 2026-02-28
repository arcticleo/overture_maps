module OvertureMaps
  class Configuration
    attr_accessor :timeout

    def initialize
      @timeout = 30
    end
  end
end
