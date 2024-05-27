# frozen_string_literal: true

require "click_house"
require "yaml"

module MVG
  module Analyser
    class Clickhouse
      attr_reader :connection

      def initialize
        config = YAML.load_file("config.yaml")

        @connection = ClickHouse::Connection.new(
          ClickHouse::Config.new(
            database: "mvg",
            url: config.dig("clickhouse", "url"),
            username: config.dig("clickhouse", "user"),
            password: config.dig("clickhouse", "password")
          )
        )
      end

      def setup_table
        connection.create_table("responses", engine: "MergeTree PRIMARY KEY id") do |t|
          t << "id String CODEC(ZSTD(3))"
          t << "datestring String CODEC(ZSTD(3))"
          t << "timestamp Int64 CODEC(ZSTD(3))"
          t << "station LowCardinality(String) CODEC(ZSTD(3))"
          t << "plannedDepartureTime DateTime CODEC(ZSTD(3))"
          t << "realtime Boolean CODEC(ZSTD(3))"
          t << "delayInMinutes Int64 CODEC(ZSTD(3))"
          t << "realtimeDepartureTime DateTime CODEC(ZSTD(3))"
          t << "transportType LowCardinality(String) CODEC(ZSTD(3))"
          t << "label LowCardinality(String) CODEC(ZSTD(3))"
          t << "divaId LowCardinality(String) CODEC(ZSTD(3))"
          t << "network LowCardinality(String) CODEC(ZSTD(3))"
          t << "trainType String CODEC(ZSTD(3))"
          t << "destination LowCardinality(String) CODEC(ZSTD(3))"
          t << "cancelled Boolean CODEC(ZSTD(3))"
          t << "sev Boolean CODEC(ZSTD(3))"
          t << "platform Int64 CODEC(ZSTD(3))"
          t << "platformChanged Boolean CODEC(ZSTD(3))"
          t << "stopPositionNumber Int64 CODEC(ZSTD(3))"
          t << "messages String CODEC(ZSTD(3))"
          t << "bannerHash String CODEC(ZSTD(3))"
          t << "occupancy LowCardinality(String) CODEC(ZSTD(3))"
          t << "stopPointGlobalId String CODEC(ZSTD(3))"
        end
      end

      def insert_rows(rows)
        connection.insert("responses", rows)
      end
    end
  end
end
