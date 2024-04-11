# frozen_string_literal: true
require "pry"
require "logger"
require "openssl"
require "securerandom"
require "ostruct"
require "gibberish"

require_relative "kafkr/encryptor"
require_relative "kafkr/message_broker"
require_relative "kafkr/log"
require_relative "kafkr/consumer"
require_relative "kafkr/producer"
require_relative "kafkr/version"

module Kafkr
  class << self
    def logger
      @logger ||= configure_logger
    end

    def configure_logger(output = STDOUT)
      begin
        @logger = ::Logger.new(output)
      rescue Errno::EACCES, Errno::ENOENT => e
        @logger = ::Logger.new(STDOUT)
        @logger.error("Could not open log file: #{e.message}")
      end
      @logger.level = ::Logger::DEBUG
      @logger
    end

    def write(message, unique_id = nil)
      begin
        logger.info(message)
      rescue IOError => e
        @logger.error("Failed to write log: #{e.message}")
      end
    end

    alias_method :log, :write
    alias_method :output, :write
    alias_method :info, :write
    alias_method :record, :write
    alias_method :trace, :write
  end

  class Error < StandardError; end

  def self.configuration
    @configuration ||= OpenStruct.new
  end

  def self.configure
    yield(configuration)
  rescue => e
    logger.error("Configuration error: #{e.message}")
  end
end

