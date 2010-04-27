require 'sonar_connector'
require 'uri'
require 'net/http'

module Sonar
  module Connector
    class SonarPushConnector < Sonar::Connector::Base
      
      attr_reader :uri
      attr_reader :batch_size
      
      def parse(settings)
        @uri = settings["uri"]
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if @uri.blank?
        
        # ensure that there's a source connector to pull data from
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'source_connector' required.") if settings["source_connector"].blank?
        
        @batch_size = settings["batch_size"] || 1000
      end
      
      def action
        source_connector.complete.move_all_to working
        
        working.files[0...batch_size].each do |filename|
          response = push(filename)
        end
      end
      
      def push(filename)
        params = JSON.parse File.read(filename)
        u = URI.parse uri
        http = Net::HTTP.new(u.host, u.port)
        http.read_timeout = 500 #is this in seconds?
        
        res = http.post u.path, {"foo"=>"bar"}
        case res
        when Net::HTTPSuccess, Net::HTTPRedirection
          working.move filename, complete
          log.info "pushed file #{filename} to #{uri}, moved to complete filestore."
        else
          log.warn "could not push file #{filename} to #{uri}. File remains in working filestore."
          res.error!
        end
      rescue Timeout::Error
        log.warn "update timeout error"
      end
      
    end
  end
end
