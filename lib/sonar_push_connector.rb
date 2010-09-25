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
        source_connectors.each {|c| c.connector_filestore.flip(:complete, filestore, :working) }
        
        begin
          count = filestore.process_batch(@batch_size, :working) do |files|
            paths = files.map{|f| filestore.file_path(:working, f)}
            begin
              log.debug"pushing #{files.length} files"
              push_batch(paths)
            rescue Exception => e
              log.warn ["caught an exception : leaving files in working area", 
                        e.class.to_s, e.message, e.backtrace].join('\n')
              raise Sonar::Connector::FileStore::LeaveInSourceArea, e.message
            end
          end
        end while count>0
      end
        
      def push_batch(files)
        params = {"messages" => files.map{|file| JSON.parse(File.read file)}.to_json,
          "connector_credentials" => connector_credentials}
        res = Net::HTTP.post_form URI.parse(uri), params
        case res
        when Net::HTTPSuccess, Net::HTTPRedirection
          log.info "pushed #{files.size} messages to #{uri}"
        else
          res.error!
        end
      end

    end
  end
end
