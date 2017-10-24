# encoding: utf-8
require "elasticsearch"
require "base64"


module LogStash
  module Filters
    class ElasticsearchClient

      attr_reader :client

      def initialize(user, password, options={})
        ssl     = options.fetch(:ssl, false)
        hosts   = options[:hosts]
        @logger = options[:logger]
        @cache_results = options[:cache_results]
        @refresh_interval = options[:refresh_interval]
        @logger.debug("---------Client.initialize, cache results: ", :@cache_results => @cache_results)

        transport_options = {}
        if user && password
          token = ::Base64.strict_encode64("#{user}:#{password.value}")
          transport_options[:headers] = { Authorization: "Basic #{token}" }
        end

        hosts.map! {|h| { host: h, scheme: 'https' } } if ssl
        # set ca_file even if ssl isn't on, since the host can be an https url
        transport_options[:ssl] = { ca_file: options[:ca_file] } if options[:ca_file]

        @logger.info("New ElasticSearch filter client", :hosts => hosts)
        @client = ::Elasticsearch::Client.new(hosts: hosts, transport_options: transport_options)
      end

      def search(params)
        # ORIGINAL: @client.search(params)

        if @cache_results
          stop = Time.now
          if @results.nil?
            @logger.info("---------Client.search, INITIALIZING ELASTICSEARCH RESULTS CACHE")
            @logger.info("Querying elasticsearch with these params ", :params => params)

            @results = @client.search(params)

            @logger.info("Caching these results from elasticsearch ", :@results => @results)
            @start = Time.now
          elsif ((stop.to_i - @start.to_i) > @refresh_interval)
            @logger.debug("---------Client.search, REFRESHING ELASTICSEARCH RESULTS CACHE because it is past the REFRESH INTERVAL: ", :@refresh_interval => @refresh_interval)

            @results = @client.search(params)

            @logger.debug("Caching these results from elasticsearch ", :@results => @results)
            @start = Time.now
          end

          results = @results
        else

          results = @client.search(params)
        end

        return results
      end
    end
  end
end
