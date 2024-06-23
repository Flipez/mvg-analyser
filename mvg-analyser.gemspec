# frozen_string_literal: true

require_relative "lib/mvg/analyser/version"

Gem::Specification.new do |spec|
  spec.name          = "mvg-analyser"
  spec.version       = MVG::Analyser::VERSION
  spec.authors       = ["Flipez"]
  spec.email         = ["code@brauser.io"]

  spec.summary       = "MVG Analyser"
  spec.description   = "MVG Analyser"
  spec.homepage      = "https://github.com/flipez/mvg-analyser"
  spec.required_ruby_version = Gem::Requirement.new(">= 3")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/flipez/mvg-analyser"
  spec.metadata["changelog_uri"] = "https://github.com/flipez/mvg-analyser"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "click_house"
  spec.add_dependency "google-cloud-bigquery"
  spec.add_dependency "minitar", "~> 0.9"
  spec.add_dependency "oj"
  spec.add_dependency "ruby-zstds", "~> 1.3"
  spec.add_dependency "thor"
  spec.add_dependency "tty-progressbar"
  spec.add_dependency "typhoeus", "~> 1.4"

  spec.add_development_dependency "rubocop", "~> 1.64"
end
