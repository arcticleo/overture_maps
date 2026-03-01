class CreateOvertureCategories < ActiveRecord::Migration[8.0]
  def change
    create_table :overture_categories do |t|
      t.string :name, null: false
      t.string :primary_category
      t.integer :hierarchy_level, default: 0

      t.timestamps
    end

    add_index :overture_categories, :name, unique: true
    add_index :overture_categories, :primary_category
  end
end
