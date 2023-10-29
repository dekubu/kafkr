# frozen_string_literal: true

require 'logger'
require 'securerandom'
require 'ostruct'
require_relative "kafkr/version"

module Kafkr
  class << self
    attr_accessor :current_environment

    # Logger methods
    def logger
      @logger ||= configure_logger
    end

    def configure_logger(output = default_output)
      @logger = ::Logger.new(output)
      set_logger_level
      @logger
    end

    def set_logger_level
      levels = {
        'development' => ::Logger::DEBUG,
        'staging'     => ::Logger::INFO,
        'production'  => ::Logger::WARN
      }
      @logger.level = levels[current_environment] || ::Logger::DEBUG
    end

    def default_output
      current_environment == 'production' ? '/var/log/kafkr.log' : STDOUT
    end

    # Environment methods
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

    # Writing logs
    def write(message, unique_id = SecureRandom.uuid)
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
end
