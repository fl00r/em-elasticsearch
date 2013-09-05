module EM::ElasticSearch
  class Cluster
    def initialize(client)
      @client = client
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-health/
    #
    def health(opts = {})
      @client.get path: "_cluster/health", query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-state/
    #
    def state(opts = {})
      @client.get path: "_cluster/state", query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-update-settings/
    #
    def settings(opts = {})
      body = Yajl.dump(opts)
      @client.put path: "_cluster/settings", body: body
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-info.html
    #
    def nodes_info(opts = {})
      host = opts.delete(:host)
      nodes = opts.delete(:nodes)
      path = if host
        "_cluster/nodes/#{host}"
      elsif nodes
        "_cluster/nodes/#{nodes * ','}"
      else
        "_cluster/nodes"
      end
      @client.get path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html
    #
    def nodes_stats(opts = {})
      host = opts.delete(:host)
      nodes = opts.delete(:nodes)
      path = if host
        "_cluster/nodes/#{host}/stats"
      elsif nodes
        "_cluster/nodes/#{nodes * ','}/stats"
      else
        "_cluster/nodes/stats"
      end
      @client.get path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html
    #
    def node_field_data_stats(opts = {})
      indices = opts.delete(:indices) || ["*"]
      path = "_nodes/stats/indices/fielddata/#{indices * ','}"
      @client.get path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html
    #
    def index_field_data_stats(opts = {})
      indices = opts.delete(:indices) || ["*"]
      path = "_stats/fielddata/#{indices * ','}"
      @client.get path: path, query: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-shutdown.html
    #
    def shutdown(opts = {})
      nodes = opts.delete :nodes
      path = if nodes
        "_cluster/nodes/#{nodes * ','}/_shutdown"
      else
        "_cluster/nodes/_shutdown"
      end
      @client.post path: path, opts: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-hot-threads/
    #
    def nodes_hot_threads(opts = {})
      nodes = opts.delete :nodes
      path = if nodes
        "_cluster/nodes/#{nodes * ','}/hot_threads"
      else
        "_cluster/nodes/hot_threads"
      end
      @client.get path: path, opts: opts
    end

    # http://www.elasticsearch.org/guide/reference/api/admin-cluster-reroute/
    #
    def cluster_reroute(opts = {})
      body = Yajl.dump(opts)
      @client.post path: "_cluster/reroute", body: body
    end
  end
end