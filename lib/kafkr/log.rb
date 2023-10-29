require "rubygems"
require "socket"
require 'file'
module Kafkr
  class MessageBroker
    attr_accessor :last_sent

    def initialize
      @subscribers = []
      @last_sent = {}
    end

    def add_subscriber(socket)
      @subscribers << socket
      @last_sent[socket] = nil
    end

    def broadcast(message)
      @subscribers.each do |subscriber|
        # Check if the socket is still open before reading
        if !subscriber.closed?
          subscriber.puts(message)
          @last_sent[subscriber] = message
        end
      rescue IOError
        # The client has disconnected, so remove them from the subscribers list
        @subscribers.delete(subscriber)
        @last_sent.delete(subscriber)
      end
    end
  end

  class Log
    def initialize(port)
      @server = TCPServer.new(port)
      @received_file = received_file
      @broker = MessageBroker.new  # Create an instance of MessageBroker
      @whitelist = load_whitelist  # Load the whitelist
    end

    def start
      loop do
        client = @server.accept
        client_ip = client.peeraddr[3]  # Get the client's IP address

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
              @broker.subscribers.delete(client)
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

    private

    def extract_uuid(message)
      match_data = /^(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}): (.+)$/.match(message)
      if match_data
        uuid = match_data[1]
        message_content = match_data[2]
        return uuid, message_content
      end
      [nil, nil]
    end

    def acknowledge_message(uuid, client)
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

    def persist_received_message(message_content)
      File.open(@received_file, "a") do |file|
        file.puts(message_content)
      end
    end

    # Define the whitelisted? method here
    def whitelisted?(ip)
      @whitelist.include?(ip.gsub("::ffff:", ""))
    end

    # Move the load_whitelist method to the Server class
    def load_whitelist
      whitelist = []

      # Add localhost as a default
      whitelist << "localhost"
      whitelist << "::1"

      if File.exist?("whitelist.txt")
        File.readlines("whitelist.txt").each do |line|
          ip = line.strip.sub(/^::ffff:/, "")  # Remove the ::ffff: prefix if present
          whitelist << ip
        end
      end

      whitelist
    end
  end
end
