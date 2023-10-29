# frozen_string_literal: true

require 'logger'
require 'securerandom'
require 'ostruct'
require_relative "kafkr/version"

module Kafkr
  class << self
    attr_accessor :current_environment

    def logger
      @logger ||= configure_logger
    end

    def configure_logger(output = nil)
      output ||= case current_environment
                 when 'production'
                   '/var/log/kafkr.log'
                 else
                   STDOUT
                 end
      @logger = ::Logger.new(output)
      @logger.level = case current_environment
                      when 'development'
                        ::Logger::DEBUG
                      when 'staging'
                        ::Logger::INFO
                      when 'production'
                        ::Logger::WARN
                      else
                        ::Logger::DEBUG
                      end
      @logger
    end
    
    
    def current_environment
      @current_environment ||= ENV['KAFKR_ENV'] || 'development'
    end

    def development?
      current_environment == 'development'
    end

    def test?
      current_environment == 'test'
    end

    def staging?
      current_environment == 'staging'
    end

    def production?
      current_environment == 'production'
    end

    def write(message, unique_id = nil)
      unique_id ||= SecureRandom.uuid
      formatted_message = "[#{unique_id}] #{message}"

      puts formatted_message if development?
      logger.info(formatted_message)
    end

    alias :log :write
    alias :output :write
    alias :info :write
    alias :record :write
    alias :trace :write
  end

  class Error < StandardError; end

  def self.configuration
    @configuration ||= OpenStruct.new
  end

  def self.configure
    yield(configuration)
  end

  # Your code goes here...
end
