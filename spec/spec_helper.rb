$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'rubygems'
require 'sonar_push_connector'

require 'sonar_connector/rspec/spec_helper'

require 'spec'
require 'spec/autorun'
require 'rr'
