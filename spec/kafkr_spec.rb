# frozen_string_literal: true

require_relative 'spec_helper'  # Adjust the path to your kafkr.rb file

RSpec.describe Kafkr do
  before(:each) do
    Kafkr.configure_logger(StringIO.new)
  end
  
  describe '#configure_logger' do
    it 'configures logger for development environment' do
      allow(Kafkr).to receive(:current_environment).and_return('development')
      Kafkr.configure_logger
      expect(Kafkr.logger.level).to eq(Logger::DEBUG)
    end

    it 'configures logger for staging environment' do
      allow(Kafkr).to receive(:current_environment).and_return('staging')
      Kafkr.configure_logger
      expect(Kafkr.logger.level).to eq(Logger::INFO)
    end

    it 'configures logger for production environment' do
      allow(Kafkr).to receive(:current_environment).and_return('production')
      Kafkr.configure_logger
      expect(Kafkr.logger.level).to eq(Logger::WARN)
    end
  end

  describe '#current_environment' do
    it 'returns the current environment' do
      ENV['KAFKR_ENV'] = 'development'
      expect(Kafkr.current_environment).to eq('development')
    end
  end

  describe '#development?' do
    it 'returns true in development environment' do
      allow(Kafkr).to receive(:current_environment).and_return('development')
      expect(Kafkr.development?).to be true
    end
  end

  describe '#test?' do
    it 'returns true in test environment' do
      allow(Kafkr).to receive(:current_environment).and_return('test')
      expect(Kafkr.test?).to be true
    end
  end

  describe '#staging?' do
    it 'returns true in staging environment' do
      allow(Kafkr).to receive(:current_environment).and_return('staging')
      expect(Kafkr.staging?).to be true
    end
  end

  describe '#production?' do
    it 'configures logger for production environment' do
      mock_logger = double("Logger")
      allow(mock_logger).to receive(:level).and_return(Logger::WARN)
      allow(Kafkr).to receive(:current_environment).and_return('production')
      allow(Logger).to receive(:new).and_return(mock_logger)
      Kafkr.configure_logger
      expect(Kafkr.logger.level).to eq(Logger::WARN)
    end
  end
  
end
