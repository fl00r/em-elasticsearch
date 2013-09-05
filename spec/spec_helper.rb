LIB_PATH = File.expand_path('../../lib/em/elasticsearch.rb',  __FILE__)

require LIB_PATH
require 'minitest/spec'
require 'minitest/autorun'
require 'minitest/reporters'
# require 'em-synchrony'

MiniTest::Reporters.use! MiniTest::Reporters::SpecReporter.new