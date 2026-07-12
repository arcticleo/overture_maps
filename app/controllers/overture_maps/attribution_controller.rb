# frozen_string_literal: true

module OvertureMaps
  # GET /overture/attribution — the notices required for the data this
  # database actually holds. Map UIs can render these verbatim.
  class AttributionController < ApplicationController
    def index
      expires_in 1.hour, public: true
      render json: {
        notices: Attribution.notices,
        text: Attribution.text
      }
    end
  end
end
