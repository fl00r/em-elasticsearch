module EM::ElasticSearch
  class Index
    def initialize(client, index, type)
      @client = client
      @index = index
      @type = type
    end

    # http://www.elasticsearch.org/guide/reference/api/get/
    #
    def get(id, source = false)
      if source
        @client.get path: [@index, @type, id, "_source"] * "/"
      else
        @client.get path: [@index, @type, id] * "/"
      end
    end

    # http://www.elasticsearch.org/guide/reference/api/get/
    #
    def exists?(id)
      @client.head path: [@index, @type, id] * "/"
    end

    # http://www.elasticsearch.org/guide/reference/api/delete/
    #
    def delete(id)
      @client.delete path: [@index, @type, id] * "/"
    end

    # http://www.elasticsearch.org/guide/reference/api/multi-get/
    #
    def multi_get(ids, fields = nil)
      body = if fields
        Yajl.dump docs: ids.map{ |id| { _id: id.to_s, fields: fields } }
      else
        Yajl.dump ids: ids.map(&:to_s)
      end
      @client.get path: [@index, @type, "_mget"] * "/", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/index_.html
    #
    def insert(id, doc)
      body = Yajl.dump doc
      @client.put path: [@index, @type, id] * "/", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/bulk/
    #
    def bulk_insert(docs)
      body = docs.map do |doc|
        id = doc.delete(:_id) or raise "Document without _id in bulk operation: #{doc.inspect}"
        [
          Yajl.dump(index: { _id: id }),
          doc
        ]
      end * "\n"
      @client.post path: [@index, @type, "_bulk"] * "/", body: body + "\n"
    end

    # http://www.elasticsearch.org/guide/reference/api/update.html
    #
    def update(id, upd)
      body = Yajl.dump upd
      @client.post path: [@index, @type, id, "_update"] * "/", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/count/
    #
    def count(doc)
      body = Yajl.dump doc
      @client.get path: [@index, @type, id, "_count"] * "/", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/search/
    #
    def search(doc)
      body = Yajl.dump doc
      @client.get path: [@index, @type, "_search"] * "/", body: body
    end
  end
end