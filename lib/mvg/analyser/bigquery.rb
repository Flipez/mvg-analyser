# frozen_string_literal: true

require "google/cloud/bigquery"

module MVG
  module Analyser
    class Bigquery
      attr_reader :bq, :dataset

      def initialize(table)
        ENV["BIGQUERY_CREDENTIALS"] = "key.json"

        @bq = Google::Cloud::Bigquery.new
        @dataset = bq.dataset "mvg"
        @table_name = table
      end

      def setup_tables
        dataset.create_table "responses" do |t|
          t.name = "MVG Responses"
          t.description = "Responses from the MVG Scraper"
          t.schema do |s|
            s.string "id"
            s.integer "datestring"
            s.integer "timestamp"
            s.string "station"
            s.integer "plannedDepartureTime"
            s.boolean "realtime"
            s.string "delayInMinutes"
            s.integer "realtimeDepartureTime"
            s.string "transportType"
            s.string "label"
            s.string "divaId"
            s.string "network"
            s.string "trainType"
            s.string "destination"
            s.boolean "cancelled"
            s.boolean "sev"
            s.integer "platform"
            s.boolean "platformChanged"
            s.integer "stopPositionNumber"
            s.json "messages"
            s.string "bannerHash"
            s.string "occupancy"
            s.string "stopPointGlobalId"
          end
        end

        dataset.create_table "requests" do |t|
          t.name = "MVG Requests"
          t.description = "Requests from the MVG Scraper"
          t.schema do |s|
            s.string "id"
            s.integer "datestring"
            s.integer "timestamp"
            s.string "station"
            s.float "appconnect_time"
            s.float "connect_time"
            s.integer "httpauth_avail"
            s.float "namelookup_time"
            s.float "pretransfer_time"
            s.string "primary_ip"
            s.integer "redirect_count"
            s.string "redirect_url"
            s.integer "request_size"
            s.string "request_url"
            s.integer "response_code"
            s.string "return_code"
            s.string "return_message"
            s.float "size_download"
            s.float "size_upload"
            s.float "starttransfer_time"
            s.float "total_time"
            s.json "headers"
            s.json "request_params"
            s.json "request_header"
          end
        end
      end

      def table
        dataset.table(table_name)
      end

      def inserter
        table.insert_async do |result|
          logger.info "#{table_name}: inserted #{result.insert_count} rows with #{result.error_count} errors"
        end
      end
    end
  end
end
