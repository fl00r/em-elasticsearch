require 'spec_helper'

describe EM::ElasticSearch::Client do
  before do
    @client = EM::ElasticSearch::Client.new(host: "http://rehub-dev2.corp.mail.ru")
  end

  it "should fire" do
    EM.run do
      req = @client.cluster.health
      req.callback do |r|
        p "!!#{r.response}"
        EM.stop
      end
    end
  end
end