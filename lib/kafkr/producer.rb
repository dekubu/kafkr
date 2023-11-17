require "readline"
require "socket"
require "fileutils"
require "securerandom"
require "ostruct"
require 'json'

module Kafkr
  module Producer
    @@file_mutex = Mutex.new

    MESSAGE_QUEUE = "./.kafkr/message_queue.txt"
    ACKNOWLEDGED_MESSAGE_QUEUE = "./.kafkr/acknowledged_messages.txt"

    def self.configuration
      FileUtils.mkdir_p "./.kafkr"
      @configuration ||= OpenStruct.new
      @configuration.host = ENV.fetch("KAFKR_HOST", "localhost")
      @configuration.port = ENV.fetch("KAFKR_PORT", "4000").to_i
      @configuration.queue_file = MESSAGE_QUEUE
      @configuration.acknowledged_file = ACKNOWLEDGED_MESSAGE_QUEUE
      @configuration.message_queue = []
      @configuration.acknowledged_messages = load_acknowledged_messages
      load_queue_from_file
      @configuration
    end

    def self.configure
      yield(configuration)
    rescue => e
      logger.error("Configuration error: #{e.message}")
    end

    def self.structured_data_to_hash(input:, sync_uid: )
      # Check the overall structure with regex and make quotes optional
      unless input.match(/\A\w+\s*(=>|<=>)\s*((\w+:\s*['"]?[^'",]*['"]?,\s*)*(\w+:\s*['"]?[^'",]*['"]?)\s*)\z/)
        return input
      end
    

      if(input.include?("<=>"))
        puts "sync message"
        # Extract the type and key-value pairs
        type, key_values_str = input.split('<=>').map(&:strip)

        puts type
        puts key_values_str
        

        key_values = key_values_str.scan(/(\w+):\s*['"]?([^'",]*)['"]?/)
      
        # Convert the array of pairs into a hash, stripping quotes if they exist
        hash_body = key_values.to_h do |key, value|
          [key.to_sym, value.strip.gsub(/\A['"]|['"]\z/, '')]
        end

        # Return the final hash with the type as the key
        { type.to_sym => hash_body, sync: true, sync_uid: sync_uid }

      else
        puts "async message"
        # Extract the type and key-value pairs
        type, key_values_str = input.split('=>').map(&:strip)
        key_values = key_values_str.scan(/(\w+):\s*['"]?([^'",]*)['"]?/)
      
        # Convert the array of pairs into a hash, stripping quotes if they exist
        hash_body = key_values.to_h do |key, value|
          [key.to_sym, value.strip.gsub(/\A['"]|['"]\z/, '')]
        end
      
        # Return the final hash with the type as the key
        { type.to_sym => hash_body }
      end

    end
  

    def self.send_message(message)
      uuid = SecureRandom.uuid

      message =  structured_data_to_hash(input: message,sync_uid: uuid)

      if message.is_a? String
        message_with_uuid = "#{uuid}: #{message}"
      end

      if message.is_a?(Hash)
        message_with_uuid = "#{uuid}: #{JSON.generate(message)}"
      end
    
      # Encrypt the message here
      encrypted_message_with_uuid = Kafkr::Encryptor.new.encrypt(message_with_uuid)
    
      begin
        if !@configuration.acknowledged_messages.include?(uuid)
          socket = TCPSocket.new(@configuration.host, @configuration.port)
          listen_for_acknowledgments(socket)
          send_queued_messages(socket)
          # Send the encrypted message instead of the plain one
          socket.puts(encrypted_message_with_uuid)
        else
          puts "Message with UUID #{uuid} has already been acknowledged. Skipping."
        end
      rescue Errno::ECONNREFUSED
        puts "Connection refused. Queuing message: #{encrypted_message_with_uuid}"
        # Queue the encrypted message
        @configuration.message_queue.push(encrypted_message_with_uuid)
        save_queue_to_file
      rescue Errno::EPIPE
        puts "Broken pipe error. Retrying connection..."
        retry_connection(encrypted_message_with_uuid)
      end

      uuid
    end
    
    private

    def self.listen_for_acknowledgments(socket)
      Thread.new do
        while line = socket.gets
          line = line.chomp
          if line.start_with?("ACK:")
            uuid = line.split(" ")[1]
            handle_acknowledgment(uuid)
          end
        end
      end
    end

    def self.handle_acknowledgment(uuid)
      @configuration.acknowledged_messages << uuid
      save_acknowledged_messages
    end

    def self.retry_connection(message_with_uuid)
      sleep(5)
      send_message(message_with_uuid)
    end

    def self.send_queued_messages(socket)
      until @configuration.message_queue.empty?
        queued_message = @configuration.message_queue.shift
        socket.puts(queued_message)
      end
    end

    def self.save_queue_to_file
      @@file_mutex.synchronize do
        File.open(@configuration.queue_file, "w") do |file|
          file.puts(@configuration.message_queue)
        end
      end
    end

    def self.load_queue_from_file
      @@file_mutex.synchronize do
        if File.exist?(@configuration.queue_file)
          @configuration.message_queue = File.readlines(@configuration.queue_file).map(&:chomp)
        end
      end
    end

    def self.load_acknowledged_messages
      @@file_mutex.synchronize do
        if File.exist?(@configuration.acknowledged_file)
          File.readlines(@configuration.acknowledged_file).map(&:chomp)
        else
          []
        end
      end
    end

    def self.save_acknowledged_messages
      @@file_mutex.synchronize do
        File.open(@configuration.acknowledged_file, "w") do |file|
          file.puts(@configuration.acknowledged_messages)
        end
      end
    end

    def self.logger
      @logger ||= Logger.new(STDOUT)
    end
  end
end
