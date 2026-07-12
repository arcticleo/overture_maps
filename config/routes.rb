# frozen_string_literal: true

OvertureMaps::Engine.routes.draw do
  resources_constraint = { resource: /places|buildings|addresses|divisions|segments|connectors|base_features/ }

  get "search", to: "search#index"
  get "attribution", to: "attribution#index"
  get "tiles/:layer/:z/:x/:y", to: "tiles#show",
                               constraints: { z: /\d+/, x: /\d+/, y: /\d+/ }

  get ":resource", to: "features#index", constraints: resources_constraint
  get ":resource/:id", to: "features#show", constraints: resources_constraint
end
