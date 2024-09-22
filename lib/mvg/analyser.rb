# frozen_string_literal: true

require_relative "analyser/bigquery"
require_relative "analyser/clickhouse"
require_relative "analyser/clickhouse_replace"
require_relative "analyser/version"

module MVG
  module Analyser
    class Error < StandardError; end
    # Your code goes here...
  end
end
