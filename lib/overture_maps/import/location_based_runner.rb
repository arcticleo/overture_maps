# frozen_string_literal: true

module OvertureMaps
  module Import
    # Orchestrates a location-based import: resolves the location (bbox
    # string or division name) to a bounding box, downloads or reuses a
    # bbox-filtered parquet extract per feature type, and streams it into the
    # target model via Runner.
    #
    # Themes map one or more feature types to models:
    #   models: { "segment" => OvertureSegment, "connector" => OvertureConnector }
    # For single-model themes, model_class: alone is enough.
    #
    # This class never prompts or exits. Interactive behavior is injected by
    # the rake layer through callbacks:
    #   select_division: ->(results) { result or nil }   nil aborts
    #   confirm_cached:  ->(path) { :use | :refresh | :abort }
    # Without callbacks (library/job usage) it picks the largest matching
    # division and reuses exact cache hits.
    class LocationBasedRunner
      attr_reader :theme, :location, :imported_count, :error_count, :errors, :bbox

      def initialize(theme:, location:, model_class: nil, models: nil, categories: nil,
                     batch_size: nil, release: nil, output_dir: nil,
                     select_division: nil, confirm_cached: nil)
        @theme = theme
        @location = location
        @models = resolve_models(model_class, models)
        @categories = categories
        @batch_size = batch_size
        @release = Releases.validate!(release || Releases.current)
        @output_dir = output_dir
        @select_division = select_division
        @confirm_cached = confirm_cached
        @imported_count = 0
        @error_count = 0
        @errors = []
      end

      def run
        @bbox = resolve_bbox
        log "Bounding box: #{bbox} (release #{@release})"

        @models.each { |type, model_class| import_type(type, model_class) }
        self
      end

      def success?
        @error_count.zero?
      end

      private

      def resolve_models(model_class, models)
        return models if models&.any?
        raise ArgumentError, "provide model_class: or models:" unless model_class

        types = Downloader.types_for_theme(theme)
        raise Error, "no types for theme: #{theme}" if types.empty?

        types.to_h { |t| [t, model_class] }
      end

      def resolve_bbox
        return location if location.is_a?(BoundingBox)

        parsed = BoundingBox.parse(location)
        return parsed if parsed

        results = DivisionSearch.search(query: location, release: @release)
        raise Error, "no divisions found matching #{location.inspect}" if results.empty?

        division = choose_division(results)
        raise CancelledError, "import cancelled" unless division

        log "Using #{division[:name]} (#{division[:subtype]}, #{[division[:country], division[:region]].compact.join(" / ")})"
        division[:bbox]
      end

      def choose_division(results)
        return results.first if results.length == 1 || @select_division.nil?

        @select_division.call(results)
      end

      def import_type(type, model_class)
        downloader = Downloader.new(theme: theme, type: type, release: @release, output_dir: @output_dir)
        path = resolve_extract(downloader)
        unless path
          log "#{theme}/#{type}: no data found"
          return
        end

        runner = Runner.new(model_class: model_class, theme: theme, type: type,
                            batch_size: @batch_size, release: @release)
        reader = ParquetReader.new(theme: theme)
        log "#{theme}/#{type}: importing #{File.basename(path)}..."
        runner.import_from_reader(reader, source: path, filter: category_filter)

        @imported_count += runner.imported_count
        @error_count += runner.error_count
        @errors.concat(runner.errors)
        log "#{theme}/#{type}: imported #{runner.imported_count}, errors #{runner.error_count}"
      end

      def resolve_extract(downloader)
        cached = downloader.cached_extract(bbox)

        if cached
          case cached_decision(cached)
          when :use
            log "Reusing cached extract #{File.basename(cached)}"
            return cached
          when :abort
            raise CancelledError, "import cancelled"
          else
            File.delete(cached)
          end
        end

        downloader.extract_bbox(bbox)
      end

      def cached_decision(path)
        return :use unless @confirm_cached

        @confirm_cached.call(path)
      end

      # Matches leaf categories against categories.primary and
      # categories.alternate. Taxonomy groups (e.g. "eat_and_drink",
      # "restaurant") expand to their leaves when the categories table has
      # been populated (rails overture_maps:categories:populate).
      def category_filter
        return nil unless @categories&.any?

        wanted = expand_categories(@categories.map(&:to_s))
        lambda do |record|
          cats = record["categories"]
          primary = cats.is_a?(Hash) ? cats["primary"] : record["basic_category"]
          alternates = cats.is_a?(Hash) ? Array(cats["alternate"]) : []

          wanted.any? { |w| primary == w || alternates.include?(w) }
        end
      end

      def expand_categories(names)
        Models::Category.expand(names)
      rescue StandardError
        names
      end

      def log(message)
        logger = OvertureMaps.configuration.logger
        logger ? logger.info(message) : puts(message)
      end
    end
  end
end
