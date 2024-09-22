# frozen_string_literal: true

require "click_house"
require "yaml"

module MVG
  module Analyser
    class ClickhouseReplace
      attr_reader :connection
      attr_accessor :cache

      def initialize
        config = YAML.load_file("config.yaml")

        @cache = []
        @connection = ClickHouse::Connection.new(
          ClickHouse::Config.new(
            database: "mvg_replace",
            url: config.dig("clickhouse", "url"),
            username: config.dig("clickhouse", "user"),
            password: config.dig("clickhouse", "password")
          )
        )
      end

      def setup_tables
        connection.create_table("responses",
                                if_not_exists: true,
                                engine: "ReplacingMergeTree",
                                order: "(responseIndex, timestamp, station)") do |t|
          t << "responseIndex Int16 CODEC(ZSTD(3))"
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

        connection.create_table("requests",
                                if_not_exists: true,
                                engine: "ReplacingMergeTree",
                                order: "(timestamp, station)") do |t|
          t << "id String CODEC(ZSTD(3))"
          t << "datestring String CODEC(ZSTD(3))"
          t << "timestamp Int64 CODEC(ZSTD(3))"
          t << "station LowCardinality(String) CODEC(ZSTD(3))"
          t << "appconnect_time Float64 CODEC(ZSTD(3))"
          t << "connect_time Float64 CODEC(ZSTD(3))"
          t << "httpauth_avail Int32 CODEC(ZSTD(3))"
          t << "namelookup_time Float64 CODEC(ZSTD(3))"
          t << "pretransfer_time Float64 CODEC(ZSTD(3))"
          t << "primary_ip LowCardinality(String) CODEC(ZSTD(3))"
          t << "redirect_count Int32 CODEC(ZSTD(3))"
          t << "redirect_url String CODEC(ZSTD(3))"
          t << "request_size Int32 CODEC(ZSTD(3))"
          t << "request_url String CODEC(ZSTD(3))"
          t << "response_code Int16 CODEC(ZSTD(3))"
          t << "return_code LowCardinality(String) CODEC(ZSTD(3))"
          t << "return_message LowCardinality(String) CODEC(ZSTD(3))"
          t << "size_download Float32 CODEC(ZSTD(3))"
          t << "size_upload Float32 CODEC(ZSTD(3))"
          t << "starttransfer_time Float32 CODEC(ZSTD(3))"
          t << "total_time Float32 CODEC(ZSTD(3))"
          t << "headers String CODEC(ZSTD(3))"
          t << "request_params String CODEC(ZSTD(3))"
          t << "request_header String CODEC(ZSTD(3))"
        end
      end

      def drop_tables
        puts "Will drop all clickhouse tables in 10s"
        sleep(10)
        connection.drop_table("responses")
        connection.drop_table("requests")
      end
    end
  end
end
