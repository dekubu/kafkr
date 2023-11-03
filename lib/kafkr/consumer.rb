require "socket"
require "timeout"
require "ostruct"
require "fileutils"

module Kafkr
  class LostConnection < StandardError; end

  class Consumer
    # Configuration setup
    def self.configuration
      FileUtils.mkdir_p "./.kafkr"
      @configuration ||= OpenStruct.new
      @configuration.host = ENV.fetch("KAFKR_HOST", "localhost")
      @configuration.port = ENV.fetch("KAFKR_PORT", "4000").to_i
      @configuration
    end

    def self.configure
      yield(configuration) if block_given?
    end

    # Consumer logic
    def initialize(host = Consumer.configuration.host, port = Consumer.configuration.port)
      @host = host
      @port = port
    end

    def fibonacci(n)
      (n <= 1) ? n : fibonacci(n - 1) + fibonacci(n - 2)
    end

    def backoff_time(attempt)
      [fibonacci(attempt), fibonacci(5)].min
    end

    def listen
      attempt = 0

      loop do
        socket = nil
        while socket.nil?
          begin
            Timeout.timeout(10) do
              socket = TCPSocket.new(@host, @port)
            end
            puts "Connected to server." if attempt == 0
            attempt = 0
          rescue Errno::ECONNREFUSED, Timeout::Error
            attempt += 1
            wait_time = backoff_time(attempt)
            puts "Failed to connect on attempt #{attempt}. Retrying in #{wait_time} seconds..."
            sleep(wait_time)
          end
        end

        begin
          loop do
            message = socket.gets
            if message.nil?
              raise LostConnection
            else
              message =  Kafkr::Encryptor.new.decrypt(message.chomp) # Decrypt the message here
              puts message.chomp
            end
          end
        rescue LostConnection
          attempt += 1
          wait_time = backoff_time(attempt)
          puts "Connection lost. Reconnecting in #{wait_time} seconds..."
          sleep(wait_time)
        rescue Interrupt
          puts "Received interrupt signal. Shutting down consumer gracefully..."
          socket.close if socket
          exit(0)
        end
      end
    end

    alias_method :consume, :listen
    alias_method :receive, :listen
    alias_method :connect, :listen
    alias_method :monitor, :listen
    alias_method :observe, :listen
  end
end
