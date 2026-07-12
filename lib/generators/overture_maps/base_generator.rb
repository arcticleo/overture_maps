# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module OvertureMaps
  module Generators
    class BaseGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      def self.default_source_root
        File.expand_path("../templates", __dir__)
      end

      private

      def migration_version
        "[#{ActiveRecord::VERSION::STRING.to_f}]"
      end
    end
  end
end
