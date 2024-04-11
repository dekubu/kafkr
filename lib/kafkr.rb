require "pry"
require "logger"
require "openssl"
require "securerandom"
require "ostruct"
require "gibberish"

# Assuming the relative paths are correct and these files exist.
require_relative "kafkr/encryptor"
require_relative "kafkr/message_broker"
require_relative "kafkr/log"
require_relative "kafkr/consumer"
require_relative "kafkr/producer"
require_relative "kafkr/version"

module Kafkr
  LOG_FILE_PATH = "/var/log/kafkr.log" # Default log file path

  class << self
    attr_writer :log_file_path

    def logger
      @logger ||= configure_logger
    end

    def log_file_path
      @log_file_path || LOG_FILE_PATH
    end

    def configure_logger
      logger = ::Logger.new(log_file_path)
      logger.level = ::Logger::DEBUG
      logger
    rescue Errno::EACCES, Errno::ENOENT => e
      # Fallback to STDOUT only if file creation fails, can be removed to strictly enforce file logging
      logger = ::Logger.new(STDOUT)
      logger.error("Could not open log file: #{e.message}")
      logger
    end

    def write(message, unique_id = nil)
      logger.info(message)
    rescue IOError => e
      logger.error("Failed to write log: #{e.message}")
    end

    # Maintaining existing method aliases
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
end