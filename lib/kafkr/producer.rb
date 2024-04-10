require "readline"
require "socket"
require "fileutils"
require "securerandom"
require "ostruct"
require "json"
require "fiber"
module Kafkr
  module Producer
    @@file_mutex = Mutex.new

    MESSAGE_QUEUE = "./.kafkr/message_queue.txt"

    def self.configuration
      FileUtils.mkdir_p "./.kafkr"
      @configuration ||= OpenStruct.new
      @configuration.queue_file = MESSAGE_QUEUE
      @configuration.message_queue = []
      load_queue_from_file
      @configuration.is_json = false
      @configuration
    end

    def self.configure
      yield(configuration)
    rescue => e
      logger.error("Configuration error: #{e.message}")
    end

    def self.structured_data_to_hash(input:, sync_uid:)
      unless /\A\w+\s*(=>|<=>)\s*((\w+:\s*['"]?[^'",]*['"]?,\s*)*(\w+:\s*['"]?[^'",]*['"]?)\s*)\z/.match?(input)
        return input
      end

      if input.include?("<=>")
        type, key_values_str = input.split("<=>").map(&:strip)
        key_values = key_values_str.scan(/(\w+):\s*['"]?([^'",]*)['"]?/)
        hash_body = key_values.to_h do |key, value|
          [key.to_sym, value.strip.gsub(/\A['"]|['"]\z/, "")]
        end
        {type.to_sym => hash_body, :sync => true, :sync_uid => sync_uid}
      else
        type, key_values_str = input.split("=>").map(&:strip)
        key_values = key_values_str.scan(/(\w+):\s*['"]?([^'",]*)['"]?/)
        hash_body = key_values.to_h do |key, value|
          [key.to_sym, value.strip.gsub(/\A['"]|['"]\z/, "")]
        end
        {type.to_sym => hash_body}
      end
    end

    def self.send_message(message)
      return if message.nil? || message.empty?

      uuid = SecureRandom.uuid
      message_with_uuid = nil

      if Kafkr::Producer.configuration.is_json
        json_message = JSON.parse(message)
        json_message["uuid"] = uuid
        message_with_uuid = JSON.dump(json_message)
      else
        if message.is_a? String
          message = structured_data_to_hash(input: message, sync_uid: uuid)
          message_with_uuid = "#{uuid}: #{message}"
        end

        if message.is_a?(Hash)
          message_with_uuid = "#{uuid}: #{JSON.generate(message)}"
        end
      end

      encrypted_message_with_uuid = Kafkr::Encryptor.new.encrypt(message_with_uuid)

      begin
        socket = TCPSocket.new(@configuration.host, @configuration.port)
        send_queued_messages(socket)
        socket.puts(encrypted_message_with_uuid)
      rescue Errno::ECONNREFUSED
        puts "Connection refused. Queuing message: #{encrypted_message_with_uuid}"
        @configuration.message_queue.push(encrypted_message_with_uuid)
        save_queue_to_file
      rescue Errno::EPIPE
        puts "Broken pipe error. Retrying connection..."
        retry_connection(encrypted_message_with_uuid)
      end

      uuid
    end

    def self.send_message_and_wait(message)
      Consumer.new.listen_for(message, method(:send_message)) do |received_message, sync_uid|
        if received_message.key? "reply" and received_message["reply"].dig("uuid") == sync_uid
          received_message["reply"].dig("payload")
        end
      end
    end

    private

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

    def self.logger
      @logger ||= Logger.new(STDOUT)
    end
  end
end
