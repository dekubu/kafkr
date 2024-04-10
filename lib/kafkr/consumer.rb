require "socket" 
require "timeout"
require "ostruct"
require "fileutils"
require "json"

module Kafkr
  class LostConnection < StandardError; end

  class Consumer
    @handlers = []

    HANDLERS_DIRECTORY = "./handlers"

    class << self
      attr_reader :handlers

      def configuration
        FileUtils.mkdir_p "./.kafkr"
        @configuration ||= OpenStruct.new
        @configuration.host = ENV.fetch("KAFKR_HOST", "localhost")
        @configuration.port = ENV.fetch("KAFKR_PORT", 4000)
        @configuration.timeout = ENV.fetch("KAFKR_CONSUMER_TIMEOUT", 300) 
        @configuration.suggest_handlers = false
        @configuration
      end

      def configure
        yield(configuration) if block_given?
      end

      def register_handler(handler)
        @handlers << handler
      end

      $loaded_handlers = {}
      $handlers_changed = true

      def list_registered_handlers
        puts "Registered handlers:"
        $loaded_handlers.keys.each { |handler| puts "- #{handler}" }
      end

      def load_handlers(directory = "./handlers")
        Dir.glob("#{directory}/*.rb").each do |file|
          handler_name = File.basename(file, ".rb")
          unless $loaded_handlers[handler_name]
            require file
            $loaded_handlers[handler_name] = true
            $handlers_changed = true
          end
        end
      end
    end

    class Handler
      def handle?(message)
        raise NotImplementedError, "You must implement the handle? method"
      end

      def handle(message)
        raise NotImplementedError, "You must implement the handle method"
      end

      def self.inherited(subclass)
        Consumer.register_handler(subclass.new)
      end

      protected

      def reply(to:, payload:)
        Kafkr::Producer.configure do |config|
          config.host = Consumer.configuration.host
          config.port = Consumer.configuration.port
        end

        Kafkr::Producer.send_message({reply: {payload: payload, uuid: to["sync_uid"]}})
      end

      private

      def can_handle?(message, name, ignore: :any)
        if message.is_a?(Numeric)
          return true if message == name.to_i
        elsif ignore == :hash
          return true if message[:message] && message[:message][:body] && message[:message][:body] == name
          return true if message[:message] && message[:message][:body] && message[:message][:body].start_with?(name)
        elsif ignore == :string
          return true if message.key? name
        else
          return true if message.key? name
          return true if message[:message] && message[:message][:body] && message[:message][:body] == name
          return true if message[:message] && message[:message][:body] && message[:message][:body].start_with?(name)
        end
        false
      end
    end

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

    def valid_class_name?(name)
      /^[A-Z]\w*$/.match?(name)
    end

    def print_handler_class(name)
      return if name.is_a?(Numeric)
      name = name.keys.first if name.is_a?(Hash)
      handler_name = "#{name.downcase}_handler"

      if $loaded_handlers.key?(handler_name)
        return
      end

      if Kafkr::Consumer.configuration.suggest_handlers
        if valid_class_name?(name.capitalize)
          puts "No handler for this message, you could use this one.\n\n"

          handler_class_string = <<~HANDLER_CLASS
            class #{name.capitalize}Handler < Kafkr::Consumer::Handler
              def handle?(message)
                can_handle? message, '#{name}'
              end

              def handle(message)
                puts message
              end
            end

            # Save the file to ./handlers/#{name}_handler.rb
          HANDLER_CLASS

          puts handler_class_string
        end
      end
    end

    require "timeout"

    def listen_for(message, send_message)
      attempt = 0
      begin
        socket = TCPSocket.new(@host, @port)
        attempt = 0

        Timeout.timeout(Kafkr::Consumer.configuration.timeout) do
          sync_uid = send_message.call(message)

          loop do
            received_message = socket.gets
            raise LostConnection if received_message.nil?
            received_message = Kafkr::Encryptor.new.decrypt(received_message.chomp)
            payload = yield received_message, sync_uid if block_given?
            return payload
          end
        end
      rescue Timeout::Error, LostConnection, Errno::ECONNREFUSED
        attempt += 1
        wait_time = backoff_time(attempt)
        sleep(wait_time)
        retry
      rescue Interrupt
        puts "Received interrupt signal. Shutting down consumer gracefully..."
        socket&.close
        exit(0)
      end
    end

    def listen
      attempt = 0
      loop do
        listen_for("dummy", ->(msg) {  }) do |message|
          Kafkr.log ">> #{message}"
          dispatch_to_handlers(message)
        end
      end
    end

    def valid_json?(json)
      JSON.parse(json)
      true
    rescue JSON::ParserError
      false
    end

    alias_method :consume, :listen
    alias_method :receive, :listen
    alias_method :connect, :listen
    alias_method :monitor, :listen
    alias_method :observe, :listen

    private

    def dispatch_to_handlers(message)
      message_hash = message.is_a?(String) ? {message: {body: message}} : message

      self.class.handlers.each do |handler|
        if handler.handle?(message_hash)
          handler.handle(message_hash)
        end
      end

      print_handler_class(message)

      yield message_hash if block_given?
    end
  end

  Consumer.load_handlers
end
