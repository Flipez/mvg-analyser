require "google/cloud/bigquery"
require 'zstds'
require 'minitar'
require 'oj'
require 'typhoeus'
require 'tty-progressbar'

module MVG
  class DataHandler
    attr_reader :bq

    def initialize
      ENV["BIGQUERY_CREDENTIALS"] = "key.json"
      
      @bq = Google::Cloud::Bigquery.new
    end

    def setup_table
      dataset = bq.dataset 'mvg'

      table = dataset.create_table "responses" do |t|
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
    end

    def stream(file, &block)
      ZSTDS::Stream::Reader.open file do |reader|
        Minitar::Reader.open reader do |tar|
          tar.each_entry(&block)
        end
      end
    end

    def full
      download
    end

    def download
      list = Typhoeus.get("https://data.mvg.auch.cool/json", followlocation: true)

      list = Oj.load(list.body)
      entries = list.select do |entry|
        entry['type'] == 'file'
      end

      puts "Found #{entries.size} archives online"
      entries.each_with_index do |entry, i|
        puts "Downloading #{i} of #{entries.size}"
        filename = "/tmp/#{entry["name"]}"

        next if File.readlines('export.log', chomp: true).include?(entry['name'])

        File.open(filename, 'wb') do |f|
          f.write Typhoeus.get("https://data.mvg.auch.cool/json/#{entry["name"]}", followlocation: true).body

          export_bq(filename)

          File.delete(f)
          File.write('export.log', "\n" + entry['name'], mode: 'a+')
          puts "Sleep 5s until next file"
          sleep(5)
        end
      end
    end

    def export_bq(file)
      puts("\texporting #{file} to BigQuery")
      bar = TTY::ProgressBar.new("inserting [:bar] :current/unknown ET:elapsed :rate/s", total: 135_000)
      dataset = bq.dataset 'mvg'
      table = dataset.table 'responses'

      inserter = table.insert_async do |result|
        if result.error?
          #p result.error
        else
          #puts "inserted #{result.insert_count} rows with #{result.error_count} errors"
        end
      end

      stream(file) do |entry|
        if entry.name.end_with? 'body.json'
          begin
            split = entry.name.split("/")
            datestring = split[0].to_i
            station = split[1]
            timestamp = split[2].split("_")[0].to_i

            content = Oj.load(entry.read)

            content = content.map do |response|
              response.each do |key, value|
                if value.is_a? Array
                  response[key] = value.to_json
                end
              end

              response["id"] = "#{datestring}-#{station}-#{timestamp}"
              response["datestring"] = datestring
              response["station"] = station
              response["timestamp"] = timestamp
              response
            end

            inserter.insert content
            bar.advance
          rescue Oj::ParseError, JSON::ParserError, TypeError => err
            #p err
          end
        end
      end

      inserter.stop.wait!
    end
  end
end