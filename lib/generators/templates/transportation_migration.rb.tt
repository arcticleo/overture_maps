# frozen_string_literal: true

class CreateOvertureTransportations < ActiveRecord::Migration[8.0]
  def change
    create_table :overture_transportations, id: :string, primary_key: :id do |t|
      t.string :names, array: true, default: []
      t.string :subtype
      t.string :class
      t.string :subclass
      t.jsonb :connectors, default: []
      t.jsonb :routes, default: []
      t.jsonb :speed_limits, default: []
      t.jsonb :access_restrictions, default: []
      t.jsonb :road_surface, default: []
      t.jsonb :road_flags, default: []
      t.jsonb :rail_flags, default: []
      t.jsonb :width_rules, default: []
      t.jsonb :level_rules, default: []
      t.jsonb :destinations, default: []
      t.jsonb :subclass_rules, default: []
      t.jsonb :prohibited_transitions, default: []
      t.timestamps
    end

    # Add geometry column using PostGIS adapter
    execute "ALTER TABLE overture_transportations ADD COLUMN geometry geometry(Geometry,4326)"
    execute "CREATE INDEX index_overture_transportations_on_geometry ON overture_transportations USING GIST (geometry)"

    add_index :overture_transportations, :subtype
    add_index :overture_transportations, :class
    add_index :overture_transportations, :subclass
  end
end
