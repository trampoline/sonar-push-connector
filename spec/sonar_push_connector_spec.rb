require 'spec_helper'

describe Sonar::Connector::SonarPushConnector do
  before do
    setup_valid_config_file
    @base_config = Sonar::Connector::Config.load(valid_config_filename)
    end
  # @connector = Sonar::Connector::SonarPushConnector.new
  
  def simple_config(opts={})
    {
      'name'=>'foobarcom-sonarpush',
      'repeat_delay'=>60,
      'uri'=>"https://foobar.com/api/1_0/json_messages",
      'connector_credentials'=>"0123456789abcdef",
      'source_connectors'=>['foo'],
      'batch_size'=>10,
      'delete'=>true
    }.merge(opts)
  end

  describe "action" do

    it "should call filestore.process_batch with :working as source and error dirs" do
      c=Sonar::Connector::SonarPushConnector.new(simple_config, @base_config)
      
      c.instance_eval{@filestore = Object.new}
     
      source_connectors = [Object.new]
      mock(c).source_connectors{source_connectors}
      mock(source_connectors[0]).connector_filestore.mock!.flip(:complete, c.filestore, :working)

      mock(c.filestore).process_batch.with_any_args{ |size, source, error, success, block|
        size.should == 10
        source.should == :working
        error.should == :working
        success.should == nil
        0
      }
      c.action
    end

    it "should call filestore.process_batch with a success area of :complete if !delete" do
      c=Sonar::Connector::SonarPushConnector.new(simple_config("delete"=>false), @base_config)
      c.delete.should == false
      
      c.instance_eval{@filestore = Object.new}
     
      source_connectors = [Object.new]
      mock(c).source_connectors{source_connectors}
      mock(source_connectors[0]).connector_filestore.mock!.flip(:complete, c.filestore, :working)

      mock(c.filestore).process_batch.with_any_args{ |size, source, error, success, block|
        size.should == 10
        source.should == :working
        error.should == :working
        success.should == :complete
        0
      }
      c.action
    end
  end
end
