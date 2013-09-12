module EM::ElasticSearch
  class Client
    DEFAULT_HOST = "http://127.0.0.1"
    DEFAULT_PORT = 9200
    DEFAULT_KEEPALIVE = true
    DEFAULT_TYPE = :em

    def initialize(opts = {})
      @host = opts[:host] || DEFAULT_HOST
      @port = opts[:port] || DEFAULT_PORT
      @keepalive = opts[:keepalive] || DEFAULT_KEEPALIVE
      @type = opts[:type] || DEFAULT_TYPE

      EM::ElasticSearch.logger.level = opts[:logger_level] || Logger::DEBUG
      @url = "http://" + [@host, @port] * ":"
    end

    def connection
      @connection = nil  unless @keepalive
      @connection ||= EM::HttpRequest.new(@url)
    end

    [:get, :post, :put, :delete, :head].each do |method|
      define_method method do |opts|
        make_request(method, opts)
      end
    end

    def cluster
      @cluster ||= EM::ElasticSearch::Cluster.new(self)
    end

    def index(name, type)
      EM::ElasticSearch::Index.new(self, name, type)
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-create-index/
    #
    def create_index(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      body = Yajl.dump(opts)
      put path: indices, body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-delete-index/
    #
    def delete_index(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      body = Yajl.dump(opts)
      delete path: indices, body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close/
    #
    def open_index(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      post path: "#{indices}/_close"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close/
    #
    def close_index(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      post path: "#{indices}/_close"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-get-settings/
    #
    def get_settings(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      get path: "#{indices}/_settings"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-update-settings/
    #
    def put_settings(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ','
      body = Yajl.dump(opts)
      put path: "#{indices}/_settings", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-get-mapping/
    #
    def get_mapping(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      types = get_opts(opts, false, :type, :types)
      path = if indices
        if types
          "#{indices * ','}/#{types * ','}/_mapping"
        else
          "#{indices * ','}/_mapping"
        end
      else
        "_mapping"
      end
      get path: "#{indices}"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-put-mapping/
    #
    def put_mapping(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, true, :type, :types) * ","
      body = Yajl.dump(opts)
      put path: "#{indices}/#{types}/_mapping", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-delete-mapping/
    #
    def delete_mapping(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, true, :type, :types) * ","
      delete path: "#{indices}/#{types}"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-refresh/
    #
    def refresh_index(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices}/_refresh" : "_refresh"
      post path: path
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-optimize/
    #
    def optimize(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices}/_optimize" : "_optimize"
      post path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-flush/
    #
    def flush_index
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices}/_flush" : "_flush"
      post path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-gateway-snapshot/
    #
    def snapshot_index(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices}/_gateway/snapshot" : "_gateway/snapshot"
      post path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-templates/
    #
    def put_template(opts = {})
      template_name = get_opts(opts, true, :name).first
      body = Yajl.dump(opts)
      put path: "_template/#{template_name}", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-templates/
    #
    def delete_template(opts = {})
      template_name = get_opts(opts, true, :name).first
      delete path: "_template/#{template_name}"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-templates/
    #
    def get_template(opts = {})
      template_name = get_opts(opts, true, :name).first
      get path: "_template/#{template_name}"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-warmers/
    #
    def put_warmer(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, false, :type, :types)
      name = get_opts(opts, true, :name).first
      body = Yajl.dump(opts)
      path = types ? "/#{indices * ','}/#{types * ','}/_warmer/#{name}" : "/#{indices * ','}/_warmer/#{name}"
      put path: path, body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-warmers/
    #
    def delete_warmer(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, false, :type, :types)
      name = get_opts(opts, false, :name) || []
      body = Yajl.dump(opts)
      path = types ? "/#{indices * ','}/#{types * ','}/_warmer/#{name.first}" : "/#{indices * ','}/_warmer/#{name.first}"
      delete path: path, body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-warmers/
    #
    def get_warmer(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, false, :type, :types)
      name = get_opts(opts, false, :name) || []
      path = types ? "/#{indices * ','}/#{types * ','}/_warmer/#{name.first}" : "/#{indices * ','}/_warmer/#{name.first}"
      get path: path
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-stats/
    #
    def index_stats(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices * ','}/_stats" : "_stats"
      get path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-status/
    #
    def index_status(opts = {})
      indices = get_opts(opts, true, :index, :indices)
      get path: "#{indices * ','}/_status"
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-segments/
    #
    def index_segmnts(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices * ','}/_segments" : "_segments"
      get path: path
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-clearcache/
    #
    def index_clear_cache(opts = {})
      indices = get_opts(opts, false, :index, :indices)
      path = indices ? "#{indices * ','}/_cache/clear" : "_cache/clear"
      post path: path
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-indices-exists/
    #
    def index_exists?(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      head path: indices
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-indices-types-exists/
    #
    def type_exists?(opts = {})
      indices = get_opts(opts, true, :index, :indices) * ","
      types = get_opts(opts, true, :type, :types) * ","
      head path: "#{indices}/#{types}"
    end

    private

    def make_request(method, opts)
      opts[:keepalive] = true  if @keepalive
      EM::ElasticSearch.logger.debug("Going to #{@host}:#{@port}/#{opts[:path]}")
      connection.send(method, opts)
    end

    def get_opts(opts, req, *keys)
      keys.each do |key|
        val = opts.delete key
        return Array(val)  if val
      end
      raise "Keys: #{keys * ' or '} not specified"  if req
    end
  end
end