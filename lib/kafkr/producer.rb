require "readline"
require "socket"
require "fileutils"
require "securerandom"
require "ostruct"

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

    def self.send_message(message)
      
      uuid = SecureRandom.uuid
      
      encrypted_message = Kafkr::Encryptor.new.encrypt(message)

      message_with_uuid = "#{uuid}: #{encrypted_message}"

      begin
        if !@configuration.acknowledged_messages.include?(uuid)
          socket = TCPSocket.new(@configuration.host, @configuration.port)
          listen_for_acknowledgments(socket)
          send_queued_messages(socket)
          socket.puts(message_with_uuid)
        else
          puts "Message with UUID #{uuid} has already been acknowledged. Skipping."
        end
      rescue Errno::ECONNREFUSED
        puts "Connection refused. Queuing message: #{message_with_uuid}"
        @configuration.message_queue.push(message_with_uuid)
        save_queue_to_file
      rescue Errno::EPIPE
        puts "Broken pipe error. Retrying connection..."
        retry_connection(message_with_uuid)
      end
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