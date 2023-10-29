# spec/main_script_spec.rb
require_relative 'spec_helper'

RSpec.describe "kafkr exe" do
  let(:kafkr_log_double) { instance_double("Kafkr::Log") }

  before do
    allow(Kafkr::Log).to receive(:new).and_return(kafkr_log_double)
    allow(kafkr_log_double).to receive(:start)
  end

  it "should start the server on the default port if no ENV is set" do
    expect(Kafkr::Log).to receive(:new).with(4000)
    expect(kafkr_log_double).to receive(:start)
    load 'exe/kafkr'
  end

  it "should start the server on the port from ENV if set" do
    allow(ENV).to receive(:[]).with("KAFKR_PORT").and_return("5000")
    expect(Kafkr::Log).to receive(:new).with(5000)
    expect(kafkr_log_double).to receive(:start)
    load 'exe/kafkr'
  end

  it "should handle Ctrl+C gracefully" do
    expect(kafkr_log_double).to receive(:start).and_raise(Interrupt)
    expect { load 'exe/kafkr'}.to output("log started!\nCtrl+C detected, shutting down...\n").to_stdout
  end

  it "should handle standard error gracefully" do
    expect(kafkr_log_double).to receive(:start).and_raise(StandardError, "Some error")
    expect { load 'exe/kafkr'}.to output("log started!\nAn error occurred: Some error\n").to_stdout
  end
end
