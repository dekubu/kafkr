require "socket"
require "rubygems"

module Kafkr
  class Log
    def initialize(port)
      @server = TCPServer.new(port)
      @broker = MessageBroker.new
      @whitelist = load_whitelist
      @acknowledged_message_ids = load_acknowledged_message_ids
    end

    def load_acknowledged_message_ids
      unless File.exist? "./.kafkr/acknowledged_message_ids.txt"
        `mkdir -p ./.kafkr`
        `touch ./.kafkr/acknowledged_message_ids.txt`
      end

      config_path = File.expand_path('./.kafkr/acknowledged_message_ids.txt')
      return [] unless File.exist?(config_path)

      File.readlines(config_path).map(&:strip)
    rescue Errno::ENOENT, Errno::EACCES => e
      puts "Error loading acknowledged_message_ids: #{e.message}"
      []
    end

    def start
      loop do
        client = @server.accept
        client_ip = client.peeraddr[3]
        unless whitelisted?(client_ip)
          puts "Connection from non-whitelisted IP: #{client_ip}. Ignored."
          client.close
          next
        end

        @broker.add_subscriber(client)
        Thread.new do
          loop do
            message = client.gets
            if message.nil?
              # Client connection has been closed
              @broker.last_sent.delete(client)
              client.close
              @broker.subscribers.delete(client) if @broker
              puts "Client connection closed. Removed from subscribers list."
              break
            else
              message = message.chomp
              uuid, message_content = extract_uuid(message)
              if uuid && message_content
                acknowledge_message(uuid, client)  # Acknowledge the message
                persist_received_message(message_content)  # Persist received messages to disk
                @broker.broadcast(message_content)
              else
                # Handle invalid message format
                puts "Received invalid message format: #{message}"
              end
            end
          end
        end
      end
    end

    def load_whitelist
      whitelist = ["localhost", "::1"]
      if File.exist?("whitelist.txt")
        File.readlines("whitelist.txt").each do |line|
          ip = line.strip.sub(/^::ffff:/, "")
          whitelist << ip
        end
      end
      whitelist
    end

    def whitelisted?(ip)
      @whitelist.include?(ip.gsub("::ffff:", ""))
    end
  
  
  private 
    def extract_uuid(message)
      match_data = /^(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}): (.+)$/.match(message)
      if match_data
        uuid = match_data[1]
        message_content = match_data[2]
        return uuid, message_content
      end
      return nil, nil
    end

    def acknowledge_message(uuid, client)
      begin
        # Implement acknowledgment logic here if needed
        puts "Received message with UUID #{uuid}. Acknowledged."
  
        # Send acknowledgment back to the producer
        acknowledgment_message = "ACK: #{uuid}"
        client.puts(acknowledgment_message)
        puts "Acknowledgment sent to producer: #{acknowledgment_message}"
      rescue Errno::EPIPE
        # Producer's socket connection has been closed, remove the subscriber
        @broker.last_sent.delete(client)
        client.close
        @broker.subscribers.delete(client)
        puts "Producer's socket connection closed. Removed from subscribers list."
      end
    end
  
    def persist_received_message(message_content)
      File.open("./.kafkr/acknowledged_message_ids.txt", 'a') do |file|
        file.puts(message_content)
      end
    end
  end
end

