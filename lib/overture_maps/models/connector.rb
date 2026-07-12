# frozen_string_literal: true

module OvertureMaps
  module Models
    # Transportation connectors: points where segments physically meet,
    # usable as routing nodes.
    class Connector < Base
      self.table_name = "overture_connectors"
    end
  end
end
