# frozen_string_literal: true

require "zstds"
require "minitar"
require "oj"
require "typhoeus"
require "logger"
require "tty-progressbar"

module MVG
  class DataHandler
    attr_reader :logger, :multibar, :entries, :exported, :req_inserter, :res_inserter

    UPSTREAM_URL = "https://data.mvg.auch.cool/json"

    def initialize
      fetch_upstream_entries
      fetch_exported

      @logger = Logger.new("logfile.log")
      @multibar = TTY::ProgressBar::Multi.new(
        "[:bar] Downloading and exporting #{entries.size - exported.size} archives | :elapsed",
        frequency: 3,
        width: 10,
        bar_format: :block,
        hide_cursor: true
      )
    end

    def connect_bigquery
      @res_inserter = Analyser::Bigquery.new("responses").inserter
      @req_inserter = Analyser::Bigquery.new("requests").inserter
    end

    def connect_clickhouse
      @res_inserter = Analyser::Clickhouse.new("responses")
      @req_inserter = Analyser::Clickhouse.new("requests")
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

    def export_bigquery
      connect_bigquery

      iterate_entries

      res_inserter.stop.wait!
      req_inserter.stop.wait!
    end

    def export_clickhouse
      connect_clickhouse

      iterate_entries

      res_inserter.commit
      req_inserter.commit
    end

    def iterate_entries
      logger.info "Found #{entries.size} upstream and #{exported.size} already exported archives"
      entries.each_with_index do |entry, _i|
        filename = entry["name"]
        next if exported.include?(filename)

        process_archive(filename)
      end
    end

    def process_archive(filename)
      filepath = "/tmp/#{filename}"

      File.open(filepath, "wb") do |f|
        f.write Typhoeus.get("#{UPSTREAM_URL}/#{filename}", followlocation: true).body

        export_file(filename, filepath)

        File.delete(f)
        File.write("export.log", "\n#{filename}", mode: "a+")
      end
    end

    def export_file(filename, filepath)
      bar = multibar.register("[:bar] #{filename} @ :rate inserts/s")

      stream(filepath) do |entry|
        if entry.name.end_with? "body.json"
          insert_response(entry, bar)
        elsif entry.name.end_with? "meta.json"
          insert_request(entry, bar)
        end
      end
    end

    def enrich_hash(entry, hash, idx = nil)
      split      = entry.name.split("/")
      datestring = split[0].to_i
      station    = split[1]
      timestamp  = split[2].split("_")[0].to_i

      hash.each do |key, value|
        hash[key] = value.to_json if value.is_a?(Array) || value.is_a?(Hash)
      end

      hash["responseIndex"] = idx if idx
      hash["datestring"] = datestring
      hash["station"] = station
      hash["timestamp"] = timestamp

      hash
    end

    def insert_response(entry, bar)
      content = Oj.load(entry.read)

      content = content.each_with_index.map do |response, idx|
        enrich_hash(entry, response, idx)
      end

      res_inserter.insert content
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

      req_inserter.insert([request])
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
