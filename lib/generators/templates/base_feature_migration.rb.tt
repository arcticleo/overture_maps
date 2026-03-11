# frozen_string_literal: true

class CreateOvertureBaseFeatures < ActiveRecord::Migration[8.0]
  def change
    create_table :overture_base_features, id: :string, primary_key: :id do |t|
      t.string :names, array: true, default: []
      t.string :subtype
      t.string :class
      t.float :height
      t.string :surface
      t.integer :depth
      t.integer :level
      t.boolean :is_salt, default: false
      t.boolean :is_intermittent, default: false
      t.integer :elevation
      t.string :wikidata
      t.jsonb :cartography, default: {}
      t.jsonb :source_tags, default: {}
      t.timestamps
    end

    # Add geometry column using PostGIS adapter
    execute "ALTER TABLE overture_base_features ADD COLUMN geometry geometry(Geometry,4326)"
    execute "CREATE INDEX index_overture_base_features_on_geometry ON overture_base_features USING GIST (geometry)"

    add_index :overture_base_features, :subtype
    add_index :overture_base_features, :class
    add_index :overture_base_features, :wikidata
  end
end
