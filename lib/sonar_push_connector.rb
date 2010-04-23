require 'sonar_connector'
require 'uri'
require 'net/http'

module Sonar
  module Connector
    class SonarPushConnector < Sonar::Connector::Base
      
      attr_reader :uri
      attr_reader :batch_size
      attr_reader :working_dir, :error_dir, :complete_dir
      
      def parse(settings)
        
        @working_dir = File.join(connector_dir, 'working')
        @error = File.join(connector_dir, 'error')
        @complete_dir = File.join(connector_dir, 'complete')
        
        @uri = settings["uri"]
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if @uri.blank?
        
        # ensure that there's a source connector to pull data from
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if settings["source_connector"].blank?
        
        @batch_size = settings["batch_size"] || 1000
      end
      
      def action
        create_folders
        
        move_all_source_dirs_into_working
        
        file_count = 0
        loop_over_current_working_dirs.each do |dir|
          
          create_same_name_in_complete
          
          file_count += 1 
          break if file_count == batch_size
        end
        
        
        Net::HTTP.post_form URI.parse(uri), 
      end
      
      # Create internal dirs for this connector instance.
      def create_dirs
        [working_dir, error_dir, complete_dir].each do |dir|
          FileUtils.mkdir_p dir unless File.directory?(dir)
        end
      end
      
      
      
    end
  end
end
