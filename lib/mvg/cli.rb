# frozen_string_literal: true

module MVG
  ###
  # Provides command line interface
  class Cli < Thor
    desc "version", "display the mvg-analyser version"
    def version
      puts("MVG Analyser version #{MVG::Analyser::VERSION}")
    end

    desc "export FILE", "exports the given compressed file into sqlite"
    def export(file)
      analyser = MVG::DataHandler.new
      analyser.export_bq(file)
    end

    desc "export-clickhouse", "download and export full upstream data to clickhouse"
    def export_clickhouse
      analyser = MVG::DataHandler.new
      analyser.export_clickhouse
    end

    desc "export-bigquery", "download and export full upstream data to bigquery"
    def export_bigquery
      analyser = MVG::DataHandler.new
      analyser.export_bigquery
    end

    desc "reset-clickhouse", "drops clickhouse tables and creates new, empty ones"
    def reset_clickhouse
      ch = Analyser::Clickhouse.new("")
      ch.drop_tables
      ch.setup_tables
    end
  end
end
