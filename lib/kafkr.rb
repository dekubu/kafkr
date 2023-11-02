# frozen_string_literal: true

require "logger"
require "securerandom"
require "ostruct"
require ''

require_relative "kafkr/message_broker"
require_relative "kafkr/log"
require_relative "kafkr/consumer.rb"
require_relative "kafkr/producer.rb"

module Kafkr
  class Encryptor
    ALGORITHM = 'AES-256-CBC'

    def initialize(key)
      @key = key
      @cipher = OpenSSL::Cipher.new(ALGORITHM)
    end

    def encrypt(data)
      @cipher.encrypt
      @cipher.key = @key
      iv = @cipher.random_iv

      encrypted_data = @cipher.update(data) + @cipher.final
      "#{iv.unpack1('m')}--#{encrypted_data.unpack1('m')}"
    end

    def decrypt(encrypted_data)
      iv, encrypted_message = encrypted_data.split('--').map(&:unpack1, 'm')
      @cipher.decrypt
      @cipher.key = @key
      @cipher.iv = iv
      @cipher.update(encrypted_message) + @cipher.final
    end
  end

  class << self
    attr_accessor :current_environment
    def logger
      @logger ||= configure_logger
    end

    def configure_logger(output = default_output)
      begin
        @logger = ::Logger.new(output)
      rescue Errno::EACCES, Errno::ENOENT => e
        @logger = ::Logger.new(STDOUT)
        @logger.error("Could not open log file: #{e.message}")
      end
      set_logger_level
      @logger
    end

    def default_output
      case current_environment
      when "production"
        "/var/log/kafkr.log"
      else
        STDOUT
      end
    end

    def set_logger_level
      @logger.level = case current_environment
      when "development"
        ::Logger::DEBUG
      when "staging"
        ::Logger::INFO
      when "production"
        ::Logger::WARN
      else
        ::Logger::DEBUG
      end
    end

    def current_environment
      @current_environment ||= ENV["KAFKR_ENV"] || "development"
    end

    def development?
      current_environment == "development"
    end

    def test?
      current_environment == "test"
    end

    def staging?
      current_environment == "staging"
    end

    def production?
      current_environment == "production"
    end

    def write(message, unique_id = nil)
      begin
        unique_id ||= SecureRandom.uuid
      rescue => e
        unique_id = "unknown"
        @logger.error("Failed to generate UUID: #{e.message}")
      end
      formatted_message = "[#{unique_id}] #{message}"

      begin
        puts formatted_message if development?
        logger.info(formatted_message)
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
