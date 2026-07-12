# frozen_string_literal: true

RSpec.describe OvertureMaps::Import::RecordMapper do
  def fake_model(columns)
    Class.new do
      define_singleton_method(:column_names) { columns }
    end
  end

  describe "places mapping" do
    let(:model) do
      fake_model(%w[id geometry name names categories primary_category brands addresses
                    sources confidence operating_status country overture_release
                    created_at updated_at])
    end
    let(:mapper) { described_class.for(theme: "places", model_class: model, release: "2026-06-17.0") }

    let(:record) do
      {
        "id" => "abc-123",
        "geometry" => "POINT (1 2)",
        "names" => { "primary" => "Cafe Ladro", "common" => { "en" => "Cafe Ladro" } },
        "categories" => { "primary" => "cafe", "alternate" => ["coffee_shop"] },
        "brand" => { "names" => { "primary" => "Ladro" } },
        "addresses" => [{ "freeform" => "600 Pine St", "country" => "US" }],
        "sources" => [{ "dataset" => "meta", "record_id" => "1" }],
        "confidence" => 0.93,
        "operating_status" => "open"
      }
    end

    it "maps the Overture struct onto flat attributes" do
      attrs = mapper.call(record)

      expect(attrs[:id]).to eq("abc-123")
      expect(attrs[:name]).to eq("Cafe Ladro")
      expect(attrs[:primary_category]).to eq("cafe")
      expect(attrs[:brands]).to eq({ "names" => { "primary" => "Ladro" } })
      expect(attrs[:country]).to eq("US")
      expect(attrs[:confidence]).to eq(0.93)
      expect(attrs[:sources]).to eq([{ "dataset" => "meta", "record_id" => "1" }])
      expect(attrs[:overture_release]).to eq("2026-06-17.0")
    end

    it "produces the same key set for sparse and full records" do
      full = mapper.call(record)
      sparse = mapper.call({ "id" => "x", "geometry" => "POINT (0 0)" })

      expect(sparse.keys).to eq(full.keys)
      expect(sparse[:brands]).to be_nil
    end

    it "drops attributes that have no column" do
      attrs = mapper.call(record)

      expect(attrs).not_to have_key(:websites)
    end

    it "reads the post-deprecation basic_category/taxonomy shape" do
      attrs = mapper.call(
        "id" => "y", "geometry" => "POINT (0 0)",
        "basic_category" => "coffee_shop",
        "taxonomy" => { "primary" => "coffee_shop", "hierarchy" => %w[eat_and_drink cafe coffee_shop] }
      )

      expect(attrs[:primary_category]).to eq("coffee_shop")
      expect(attrs[:categories]).to eq(
        { "primary" => "coffee_shop", "hierarchy" => %w[eat_and_drink cafe coffee_shop] }
      )
    end
  end

  describe "buildings mapping" do
    let(:model) do
      fake_model(%w[id geometry name names subtype building_class height num_floors level
                    is_underground sources overture_release created_at updated_at])
    end
    let(:mapper) { described_class.for(theme: "buildings", model_class: model) }

    it "maps class to building_class" do
      attrs = mapper.call(
        "id" => "b1", "class" => "apartments", "subtype" => "residential",
        "height" => 21.5, "num_floors" => 6
      )

      expect(attrs[:building_class]).to eq("apartments")
      expect(attrs[:subtype]).to eq("residential")
      expect(attrs[:height]).to eq(21.5)
      expect(attrs[:num_floors]).to eq(6)
      expect(attrs).not_to have_key(:class)
    end
  end

  describe "divisions mapping (division_area)" do
    let(:model) do
      fake_model(%w[id geometry name names division_id subtype division_class country region
                    is_land is_territorial bbox_xmin bbox_xmax bbox_ymin bbox_ymax
                    sources overture_release created_at updated_at])
    end
    let(:mapper) { described_class.for(theme: "divisions", type: "division_area", model_class: model) }

    it "flattens the bbox struct into columns" do
      attrs = mapper.call(
        "id" => "area-1", "division_id" => "div-1",
        "names" => { "primary" => "Seattle" }, "subtype" => "locality", "class" => "land",
        "country" => "US", "region" => "US-WA", "is_land" => true,
        "bbox" => { "xmin" => -122.4, "xmax" => -122.2, "ymin" => 47.5, "ymax" => 47.7 }
      )

      expect(attrs[:division_id]).to eq("div-1")
      expect(attrs[:division_class]).to eq("land")
      expect(attrs[:bbox_xmin]).to eq(-122.4)
      expect(attrs[:bbox_ymax]).to eq(47.7)
    end

    it "handles missing bbox structs" do
      attrs = mapper.call("id" => "area-2")

      expect(attrs[:bbox_xmin]).to be_nil
    end
  end

  describe "transportation mapping" do
    let(:segment_model) do
      fake_model(%w[id geometry name names subtype segment_class subclass connectors routes
                    speed_limits sources overture_release created_at updated_at])
    end
    let(:connector_model) do
      fake_model(%w[id geometry sources overture_release created_at updated_at])
    end

    it "maps segment rule arrays and class to segment_class" do
      mapper = described_class.for(theme: "transportation", type: "segment", model_class: segment_model)
      attrs = mapper.call(
        "id" => "s1", "subtype" => "road", "class" => "residential",
        "connectors" => [{ "connector_id" => "c1", "at" => 0.0 }],
        "speed_limits" => [{ "max_speed" => { "value" => 50 } }]
      )

      expect(attrs[:segment_class]).to eq("residential")
      expect(attrs[:connectors].first["connector_id"]).to eq("c1")
      expect(attrs[:speed_limits]).not_to be_nil
    end

    it "maps connectors to bare id + geometry rows" do
      mapper = described_class.for(theme: "transportation", type: "connector", model_class: connector_model)
      attrs = mapper.call("id" => "c1", "geometry" => "POINT (1 2)")

      expect(attrs.keys).to match_array(%i[id geometry sources overture_release created_at updated_at])
    end
  end

  describe "base theme mapping" do
    let(:model) do
      fake_model(%w[id geometry name names feature_type subtype feature_class surface elevation
                    level wikidata source_tags sources overture_release created_at updated_at])
    end

    it "stamps the feature_type discriminator from the imported type" do
      mapper = described_class.for(theme: "base", type: "water", model_class: model)
      attrs = mapper.call(
        "id" => "w1", "subtype" => "lake", "class" => "lake",
        "names" => { "primary" => "Lake Union" }, "source_tags" => { "natural" => "water" }
      )

      expect(attrs[:feature_type]).to eq("water")
      expect(attrs[:feature_class]).to eq("lake")
      expect(attrs[:name]).to eq("Lake Union")
      expect(attrs[:source_tags]).to eq({ "natural" => "water" })
    end
  end

  describe "addresses mapping" do
    let(:model) do
      fake_model(%w[id geometry number street unit locality region postcode country
                    postal_city address_levels sources overture_release created_at updated_at])
    end
    let(:mapper) { described_class.for(theme: "addresses", model_class: model) }

    it "maps flat fields and derives region/locality from address_levels" do
      attrs = mapper.call(
        "id" => "a1", "number" => "600", "street" => "Pine Street", "postcode" => "98101",
        "country" => "US",
        "address_levels" => [{ "value" => "WA" }, { "value" => "Seattle" }]
      )

      expect(attrs[:number]).to eq("600")
      expect(attrs[:region]).to eq("WA")
      expect(attrs[:locality]).to eq("Seattle")
    end

    it "prefers postal_city for locality" do
      attrs = mapper.call("id" => "a2", "postal_city" => "Shoreline",
                          "address_levels" => [{ "value" => "WA" }, { "value" => "Seattle" }])

      expect(attrs[:locality]).to eq("Shoreline")
    end
  end
end
