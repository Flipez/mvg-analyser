# frozen_string_literal: true

require "google/cloud/bigquery"
require "zstds"
require "minitar"
require "oj"
require "typhoeus"
require "logger"
require "tty-progressbar"
require "tty-prompt"

module MVG
  class DataHandler
    attr_reader :logger, :multibar, :entries, :exported, :prompt, :req_inserter, :res_inserter, :clickhouse

    UPSTREAM_URL = "https://data.mvg.auch.cool/json"

    def initialize
      ENV["BIGQUERY_CREDENTIALS"] = "key.json"

      fetch_upstream_entries
      fetch_exported

      #connect_bigquery

      @logger = Logger.new("logfile.log")
      @multibar = TTY::ProgressBar::Multi.new(
        "[:bar] Downloading and exporting #{entries.size - exported.size} archives | :elapsed",
        frequency: 3,
        width: 10,
        bar_format: :block,
        hide_cursor: true
      )
      @prompt = TTY::Prompt.new

      @clickhouse = Analyser::Clickhouse.new
    end

    def connect_bigquery
      Google::Apis.logger = logger
      bq = Google::Cloud::Bigquery.new
      dataset = bq.dataset "mvg"

      response_table = dataset.table "responses"
      request_table = dataset.table "requests"

      @res_inserter = response_table.insert_async do |result|
        logger.info "responses: inserted #{result.insert_count} rows with #{result.error_count} errors"
      end
      @req_inserter = request_table.insert_async(max_rows: 5_000) do |result|
        logger.info "requests: inserted #{result.insert_count} rows with #{result.error_count} errors"
        logger.error result.insert_errors if result.error_count.positive?
      end
    end

    def setup_table
      bq = Google::Cloud::Bigquery.new
      dataset = bq.dataset "mvg"

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

    def fetch_upstream_entries
      response = Typhoeus.get(UPSTREAM_URL, followlocation: true)

      list = Oj.load(response.body)
      @entries = list.select do |entry|
        entry["type"] == "file"
      end
    end

    def fetch_exported
      @exported = File.readlines("export.log", chomp: true)
    end

    def stream(file, &block)
      ZSTDS::Stream::Reader.open file do |reader|
        Minitar::Reader.open reader do |tar|
          tar.each_entry(&block)
        end
      end
    end

    def full
      iterate_entries
    end

    def iterate_entries
      logger.info "Found #{entries.size} upstream and #{exported.size} already exported archives"
      entries.each_with_index do |entry, _i|
        filename = entry["name"]
        next if exported.include?(filename)

        process_archive(filename)
      end

      res_inserter.stop.wait!
      req_inserter.stop.wait!
    end

    def process_archive(filename)
      filepath = "/tmp/#{filename}"

      File.open(filepath, "wb") do |f|
        f.write Typhoeus.get("#{UPSTREAM_URL}/#{filename}", followlocation: true).body

        export_bq(filename, filepath)

        File.delete(f)
        File.write("export.log", "\n#{filename}", mode: "a+")

        exit unless prompt.yes?("Continue next record?")
      end
    end

    def export_bq(filename, filepath)
      bar = multibar.register("[:bar] #{filename} @ :rate inserts/s")

      cache = []
      stream(filepath) do |entry|
        if entry.name.end_with? "body.json"
          insert_response(entry, bar, cache)
        elsif entry.name.end_with? "meta.json"
          #insert_request(entry, bar)
        end

        if cache.size > 5000
          clickhouse.insert_rows(cache)
          cache = []
        end
      end
    end

    def enrich_hash(entry, hash)
      split      = entry.name.split("/")
      datestring = split[0].to_i
      station    = split[1]
      timestamp  = split[2].split("_")[0].to_i

      hash.each do |key, value|
        hash[key] = value.to_json if value.is_a?(Array) || value.is_a?(Hash)
      end

      hash["id"] = "#{datestring}-#{station}-#{timestamp}"
      hash["datestring"] = datestring
      hash["station"] = station
      hash["timestamp"] = timestamp

      hash
    end

    def insert_response(entry, bar, cache)
      content = Oj.load(entry.read)

      content = content.map do |response|
        enrich_hash(entry, response)
      end

      #res_inserter.insert content
      #clickhouse.insert_rows(content)
      cache.concat content
      bar.advance
    rescue Oj::ParseError, JSON::ParserError, TypeError => e
      case e.message
      when "Nil is not a valid JSON source."
        nil
      when "unexpected character (after ) at line 1, column 1 [parse.c:762]"
        nil
      else
        logger.error "#{entry.name} #{e.class} #{e.message}"
      end
    end

    def insert_request(entry, bar)
      request = Oj.load(entry.read)

      enrich_hash(entry, request)

      req_inserter.insert request
      bar.advance
    rescue Oj::ParseError, JSON::ParserError, TypeError => e
      case e.message
      when "Nil is not a valid JSON source."
        nil
      else
        logger.error "#{entry.name} #{e.class} #{e.message}"
      end
    end
  end
end
