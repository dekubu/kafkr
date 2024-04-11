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
      @configuration.is_json = true
      @configuration
    end

    def self.configure
      yield(configuration)
    rescue => e
      logger.error("Configuration error: #{e.message}")
    end

    def self.send_message(message)
      return if message.nil? || message.empty?

      uuid = SecureRandom.uuid
      message_with_uuid = nil

      if Kafkr::Producer.configuration.is_json
        json_message = JSON.parse(message)
        json_message["uuid"] = uuid
        message_with_uuid = JSON.dump(json_message)
      end

      encrypted_message_with_uuid = Kafkr::Encryptor.new.encrypt(message_with_uuid)

      begin
        socket = TCPSocket.new(@configuration.host, @configuration.port)
        send_queued_messages(socket)
        socket.puts (encrypted_message_with_uuid)
      rescue Errno::ECONNREFUSED
        Kafkr.log "Connection refused. Queuing message: #{encrypted_message_with_uuid}"
        @configuration.message_queue.push(encrypted_message_with_uuid)
        save_queue_to_file
      rescue Errno::EPIPE
        Kafkr.log "Broken pipe error. Retrying connection..."
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

  end
end
