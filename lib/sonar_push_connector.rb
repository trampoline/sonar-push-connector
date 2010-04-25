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
        @error_dir = File.join(connector_dir, 'error')
        @complete_dir = File.join(connector_dir, 'complete')
        
        @uri = settings["uri"]
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if @uri.blank?
        
        # ensure that there's a source connector to pull data from
        raise Sonar::Connector::InvalidConfig.new("Connector '#{name}': parameter 'uri' required.") if settings["source_connector"].blank?
        
        @batch_size = settings["batch_size"] || 1000
      end
      
      def action
        create_base_dirs
        op_working, op_error, op_complete = create_op_dirs working_dir
        move_all source_connector.complete_dir, op_working
        
        files = Dir.chdir(op_working){ Dir['**/*'] }[0...batch_size]
        files.each do |file|
          log.info file
          # relative_dir = File.dirname(file)
          # 
          # working/working_232323
          # create_same_name_in_complete
          # response = push_file(file)
          # case response
          # when Good
          #   
          # 
        end
        
        
        
      end
      
      # Create internal dirs for this connector instance.
      def create_base_dirs
        [working_dir, error_dir, complete_dir].each do |dir|
          FileUtils.mkdir_p dir unless File.directory?(dir)
        end
      end
      
      # Create and return op dirs inside a working dir.
      def create_op_dirs(working_dir)
        ["op_working", "op_error", "op_complete"].map do |prefix|
          dir = File.join working_dir, Sonar::Connector::Utils.timestamped_id(prefix)
          FileUtils.mkdir_p(dir) unless File.directory?(dir)
          dir
        end
      end
      
      # Move all files and dirs from source dir to target dir
      def move_all(source_dir, target_dir)
        [source_dir, target_dir].each {|dir| raise "dir doesn't exist" unless File.directory?(dir)}
        
        Dir[File.join source_dir, "*"].each do |f|
          FileUtils.mv(f, target_dir)
        end
      end
        
      def push_file
        # Net::HTTP.post_form URI.parse(uri), 
      end
      
    end
  end
end
