# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'em/elasticsearch/version'

Gem::Specification.new do |spec|
  spec.name          = "em-elasticsearch"
  spec.version       = EM::ElasticSearch::VERSION
  spec.authors       = ["Peter Yanovich"]
  spec.email         = ["fl00r@yandex.ru"]
  spec.description   = %q{EventMachine ElasticSearch Client}
  spec.summary       = %q{EventMachine ElasticSearch Client}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"

  spec.add_dependency "eventmachine"
  spec.add_dependency "em-http-request"
  spec.add_dependency "yajl-ruby"
end
