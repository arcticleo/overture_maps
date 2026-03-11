# frozen_string_literal: true

class CreateOvertureDivisions < ActiveRecord::Migration[8.0]
  def change
    create_table :overture_divisions, id: :string, primary_key: :id do |t|
      t.string :names, array: true, default: []
      t.string :subtype
      t.string :class
      t.string :country
      t.string :region
      t.string :division_id
      t.string :parent_division_id
      t.integer :population
      t.boolean :is_land, default: false
      t.boolean :is_territorial, default: false
      t.boolean :is_disputed, default: false
      t.integer :admin_level
      t.jsonb :local_type, default: {}
      t.jsonb :hierarchies, default: {}
      t.jsonb :perspectives, default: {}
      t.jsonb :norms, default: {}
      t.jsonb :capital_division_ids, default: []
      t.jsonb :capital_of_divisions, default: []
      t.string :wikidata
      t.timestamps
    end

    # Add geometry column using PostGIS adapter
    execute "ALTER TABLE overture_divisions ADD COLUMN geometry geometry(Geometry,4326)"
    execute "CREATE INDEX index_overture_divisions_on_geometry ON overture_divisions USING GIST (geometry)"

    add_index :overture_divisions, :subtype
    add_index :overture_divisions, :country
    add_index :overture_divisions, :region
    add_index :overture_divisions, :parent_division_id
    add_index :overture_divisions, :admin_level
    add_index :overture_divisions, :wikidata
  end
end
