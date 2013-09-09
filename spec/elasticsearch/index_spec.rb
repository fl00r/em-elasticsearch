require 'spec_helper'

describe ::EM::ElasticSearch::Index do
  before do
    @client = ::EM::ElasticSearch::Client.new(host: "http://rehub-dev2.corp.mail.ru")
    @index = @client.index("videos", "acc")
  end

  it "should fire" do
    ::EM.run do
      req = @index.insert( 1, title: "Pedro" )
      req.callback do |res|
        @client.refresh_index.callback do
          req = @index.search query: { match: { title: "Pedro" } }
          req.callback do |res|
            resp = Yajl.load res.response
            resp["hits"]["total"].must_equal 1
            EM.stop
          end
        end
      end
    end
  end
end