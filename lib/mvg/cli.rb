# frozen_string_literal: true

module MVG
  ###
  # Provides command line interface
  class Cli < Thor
    desc "version", "display the stan version"
    def version
      puts("MVG Scraper version #{MVG::Analyser::VERSION}")
    end

    desc "export FILE", "exports the given compressed file into sqlite"
    def export(file)
      analyser = MVG::DataHandler.new
      analyser.export_bq(file)
    end

    desc "full", "download and export full upstream data"
    def full
      analyser = MVG::DataHandler.new
      analyser.full
    end
  end
end
