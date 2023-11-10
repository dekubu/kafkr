require "socket"
require "timeout"
require "ostruct"
require "fileutils"
require "json"

module Kafkr
  class LostConnection < StandardError; end


  class Consumer
    @handlers = []
    
    HANDLERS_DIRECTORY="./handlers"

    class << self
      attr_reader :handlers

      def configuration
        FileUtils.mkdir_p "./.kafkr"
        @configuration ||= OpenStruct.new
        @configuration.host = ENV.fetch("KAFKR_HOST", "localhost")
        @configuration.port = ENV.fetch("KAFKR_PORT", "4000").to_i
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
        # Load handlers and check for new additions
        Dir.glob("#{directory}/*.rb").each do |file|
          handler_name = File.basename(file, '.rb')
          unless $loaded_handlers[handler_name]
            require file
            $loaded_handlers[handler_name] = true
            $handlers_changed = true
          end
        end
  
        # Display handlers if there are changes
        if $handlers_changed
          $handlers_changed = false
        end
      end
  
   
        
    end

    class Handler
      def handle?(message)
        false
      end
    
      def handle(message)
        raise NotImplementedError, 'You must implement the handle method'
      end

      # ... rest of your existing Handler methods ...
      def self.inherited(subclass)
        Consumer.register_handler(subclass.new)
      end
      private
      def can_handle?(name)
        message.key?(:message) && message[:message].key?(:body) && message[:message][:body].start_with?(name)
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

    def print_handler_class(name)
      handler_class_string = <<~HANDLER_CLASS

        class #{name.capitalize} < Kafkr::Consumer::Handler
          def handle?(message)
            can_handle? #{name}
          end
    
          def handle(message)
            puts message
          end
        end

      HANDLER_CLASS
    
      puts handler_class_string
    end
    
    def listen
      attempt = 0
      loop do
        begin
          socket = TCPSocket.new(@host, @port)
          puts "Connected to server." if attempt == 0
          attempt = 0

          loop do
            message = socket.gets
            raise LostConnection if message.nil?

            # Assuming Kafkr::Encryptor is defined elsewhere
            message = Kafkr::Encryptor.new.decrypt(message.chomp) 
            if valid_json?(message)
               dispatch_to_handlers(JSON.parse(message)) do |message|
                 yield message if block_given?
               end
            else
              dispatch_to_handlers(message) do |message|
                yield message if block_given?
              end
            end
          end
        rescue LostConnection
          attempt += 1
          wait_time = backoff_time(attempt)
          puts "Connection lost. Reconnecting in #{wait_time} seconds..."
          sleep(wait_time)
        rescue Errno::ECONNREFUSED, Timeout::Error
          attempt += 1
          wait_time = backoff_time(attempt)
          puts "Failed to connect on attempt #{attempt}. Retrying in #{wait_time} seconds..."
          sleep(wait_time)
        rescue Interrupt
          puts "Received interrupt signal. Shutting down consumer gracefully..."
          socket&.close
          exit(0)
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
  message_handled = false
  message_hash = message.is_a?(String) ? { message: { body: message } } : message

  self.class.handlers.each do |handler|
    if handler.handle?(message_hash)
      handler.handle(message_hash)
      message_handled = true
    end
  end

  unless message_handled
    puts "No handler for this message, you could use this one."
    print_handler_class(message)
    puts message
  end

  yield message_hash if block_given?
end

  end

  # Assuming the handlers directory is the default location
  Consumer.load_handlers
end
