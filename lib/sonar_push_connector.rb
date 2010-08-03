require 'sonar_connector'
require 'uri'
require 'net/http'

module Sonar
  module Connector
    class SonarPushConnector < Sonar::Connector::Base
      
      attr_reader :uri
      attr_reader :batch_size
      attr_reader :connector_credentials
      
      def parse(settings)
        @uri = settings["uri"]
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if @uri.blank?

        @connector_credentials = settings["connector_credentials"]
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'connector_credentials' required") if @connector_credentials.blank?
        
        # ensure that there's a source connector to pull data from
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'source_connector' required.") if settings["source_connector"].blank?
        

        @batch_size = settings["batch_size"] || 50
      end
      
      def action
        source_connector.complete.move_all_to working
        
        files = working.files[0...batch_size]
        
        if files.empty?
          log.info "Nothing to do."
          return
        end
        
        params = {"messages" => files.map{|file| JSON.parse(File.read file)}.to_json,
          "connector_credentials" => connector_credentials}
        begin
          res = Net::HTTP.post_form URI.parse(uri), params
          case res
          when Net::HTTPSuccess, Net::HTTPRedirection
            files.each {|f| working.move f, complete}
            log.info "pushed #{files.size} messages to #{uri} and moved associated files to complete filestore."
          else
            res.error!
          end
        rescue Timeout::Error
          log.warn "caught a timeout error, re-raising"
          raise $!
        rescue
          log.warn "could not post to #{uri}. Files remain in working filestore."
          raise $!
        end
      end
      
    end
  end
end
