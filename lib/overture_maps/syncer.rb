# frozen_string_literal: true

module OvertureMaps
  # Brings imported areas up to a newer Overture release without full
  # reloads. For each tracked area (see Models::ImportedArea):
  #
  # 1. When every intermediate release's changelog is known, delete the ids
  #    marked `removed` in each step (bbox-scoped), then re-extract and
  #    upsert the area at the target release — upserts cover `added` and
  #    `data_changed`, deletions cover `removed`.
  # 2. When the chain is incomplete (the area's release is no longer in the
  #    catalog), fall back to a full refresh: delete the area's rows and
  #    re-import.
  #
  # Overlapping areas sharing a table converge once all areas are synced to
  # the same target.
  class Syncer
    Result = Struct.new(:area, :status, :removed, :imported, :errors, :message, keyword_init: true)

    DELETE_BATCH_SIZE = 1000

    def initialize(target_release: nil)
      @target = Releases.validate!(target_release || Releases.current)
    end

    attr_reader :target

    def sync_all
      Models::ImportedArea.find_each.map { |area| sync_area(area) }
    end

    def sync_area(area)
      return Result.new(area: area, status: :up_to_date, removed: 0, imported: 0, errors: 0) if area.release == target

      model = area.model_class
      bbox = area.to_bounding_box
      steps = releases_between(area.release, target)

      if steps
        removed = delete_removed(area, model, bbox, steps)
        runner = reimport(area, model, bbox)
        finalize(area, runner)
        Result.new(area: area, status: :synced, removed: removed,
                   imported: runner.imported_count, errors: runner.error_count)
      else
        purged = purge_area(model, bbox)
        runner = reimport(area, model, bbox)
        finalize(area, runner)
        Result.new(area: area, status: :refreshed, removed: purged,
                   imported: runner.imported_count, errors: runner.error_count,
                   message: "changelog chain from #{area.release} unavailable; did a full refresh")
      end
    rescue StandardError => e
      Result.new(area: area, status: :failed, removed: 0, imported: 0, errors: 1, message: e.message)
    end

    # The releases after `from` up to and including `to`, oldest first, or
    # nil when the chain can't be established from the known release list.
    def releases_between(from, to)
      known = Releases.all.sort
      from_idx = known.index(from)
      to_idx = known.index(to)
      return nil if from_idx.nil? || to_idx.nil? || to_idx <= from_idx

      known[(from_idx + 1)..to_idx]
    end

    private

    def delete_removed(area, model, bbox, steps)
      total = 0
      steps.each do |release|
        ids = Changelog.removed_ids(theme: area.theme, type: area.feature_type,
                                    release: release, bbox: bbox)
        ids.each_slice(DELETE_BATCH_SIZE) do |slice|
          total += model.where(id: slice).delete_all
        end
      end
      total
    end

    def purge_area(model, bbox)
      model.within_bounds(bbox.min_lat, bbox.min_lng, bbox.max_lat, bbox.max_lng).delete_all
    end

    def reimport(area, model, bbox)
      Import::LocationBasedRunner.new(
        theme: area.theme,
        location: bbox,
        models: { area.feature_type => model },
        release: target
      ).run
    end

    def finalize(area, runner)
      area.update!(release: target, records_count: runner.imported_count)
    end
  end
end
