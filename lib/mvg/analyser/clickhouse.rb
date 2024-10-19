# frozen_string_literal: true

require "click_house"
require "yaml"
require "typhoeus"
require "json"

module MVG
  module Analyser
    class Clickhouse
      attr_reader :connection, :table
      attr_accessor :cache

      def initialize(table)
        config = YAML.load_file("config.yaml")

        @cache = []
        @connection = ClickHouse::Connection.new(
          ClickHouse::Config.new(
            database: "mvg",
            url: config.dig("clickhouse", "url"),
            username: config.dig("clickhouse", "user"),
            password: config.dig("clickhouse", "password")
          )
        )
        @table = table
      end

      def setup_tables
        connection.create_table("responses",
                                if_not_exists: true,
                                engine: "MergeTree",
                                partition: "datestring",
                                order: "(label, destination, station, plannedDepartureTime, timestamp)") do |t|
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
                                order: "(station, timestamp)",
                                partition: "datestring",
                                engine: "MergeTree") do |t|
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

        connection.create_table("stations", if_not_exists: true, engine: "MergeTree", order: "(name)") do |t|
          t << "name String CODEC(ZSTD(3))"
          t << "place String CODEC(ZSTD(3))"
          t << "id String CODEC(ZSTD(3))"
          t << "divaId Int32 CODEC(ZSTD(3))"
          t << "abbreviation String CODEC(ZSTD(3))"
          t << "tariffZones LowCardinality(String) CODEC(ZSTD(3))"
          t << "products String CODEC(ZSTD(3))"
          t << "latitude Float64 CODEC(ZSTD(3))"
          t << "longitude Float64 CODEC(ZSTD(3))"
        end
      end

      def drop_tables
        puts "Will drop all clickhouse tables in 10s"
        sleep(10)
        connection.drop_table("responses")
        connection.drop_table("requests")
        connection.drop_table("stations")
      end

      def insert(rows)
        cache.concat(rows)

        return unless cache.size > 100_000

        commit
      end

      def commit
        connection.insert(table, cache)
        @cache = []
      end

      def update_stations
        response = Typhoeus.get("https://www.mvg.de/.rest/zdm/stations")

        stations = JSON.parse(response.body)

        cache = []

        stations.each do |station|
          station.each do |key, value|
            station[key] = value.to_json if value.is_a?(Array) || value.is_a?(Hash)
          end

          cache << station
        end

        connection.insert("stations", cache)
      end
    end
  end
end
