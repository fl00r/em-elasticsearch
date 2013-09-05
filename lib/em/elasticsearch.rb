require 'eventmachine'
require 'em-http-request'
require 'logger'
require File.expand_path("../elasticsearch/client", __FILE__)
require File.expand_path("../elasticsearch/cluster", __FILE__)

module EM::ElasticSearch
  extend self

  def logger
    @logger ||= begin
      l = Logger.new(STDOUT)
      l.formatter = Logger::Formatter.new
      l
    end
  end
end