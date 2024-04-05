require "socket"
require "rubygems"

module Kafkr
  class Log
    def initialize(port)
      @server = TCPServer.new(port)
      @received_file = "./.kafkr/log.txt"
      @broker = MessageBroker.new
      @whitelist = load_whitelist
    end

    def load_whitelist
      whitelist = ["localhost", "::1", "127.0.0.1"]
      if File.exist?("whitelist.txt")
        File.readlines("whitelist.txt").each do |line|
          ip = line.strip.sub(/^::ffff:/, "")
          whitelist << ip
        end
      end
      whitelist
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
            encrypted_message = client.gets
            if encrypted_message.nil?
              @broker.last_sent.delete(client)
              client.close
              @broker.subscribers.delete(client)
              puts "Client connection closed. Removed from subscribers list."
              break
            else
              decryptor = Kafkr::Encryptor.new
              message = decryptor.decrypt(encrypted_message.chomp) # Decrypt the message here
              uuid, message_content = extract_uuid(message)
              if uuid && message_content
                @broker.broadcast(message_content)
              else
                puts "Received invalid message format: #{message}"
              end
            end
          rescue Errno::ECONNRESET
            puts "Connection reset by client. Closing connection..."
            client.close
          end
        end
      end
    end

    def whitelisted?(ip)
      @whitelist.include?(ip.gsub("::ffff:", ""))
    end

    private

    def extract_uuid(message)
      # Check if message is valid JSON
      begin
        message = JSON.parse(message)
        return message["uuid"], message
      rescue JSON::ParserError => e
        puts "Received invalid message format: #{message}"
        match_data = /^(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}): (.+)$/.match(message)
        match_data ? [match_data[1], match_data[2]] : [nil, nil]
      end
    end
  end
end
