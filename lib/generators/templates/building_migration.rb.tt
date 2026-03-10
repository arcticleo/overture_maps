# frozen_string_literal: true

class CreateOvertureBuildings < ActiveRecord::Migration[8.0]
  def change
    create_table :overture_buildings, id: :string, primary_key: :id do |t|
      t.string :names, array: true, default: []
      t.float :height
      t.integer :level
      t.string :class
      t.boolean :is_underground, default: false
      t.timestamps
    end

    # Add geometry column using PostGIS adapter
    # Using generic geometry type to support Polygon, MultiPolygon, etc.
    execute "ALTER TABLE overture_buildings ADD COLUMN geometry geometry(Geometry,4326)"
    execute "CREATE INDEX index_overture_buildings_on_geometry ON overture_buildings USING GIST (geometry)"

    add_index :overture_buildings, :height
    add_index :overture_buildings, :level
    add_index :overture_buildings, :class
  end
end
