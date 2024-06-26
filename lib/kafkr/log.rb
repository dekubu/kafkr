require "socket"
require "rubygems"
require "kafkr"


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
          Kafkr.log "Connection from non-whitelisted IP: #{client_ip}. Ignored."
          client.close
          next
        end

        @broker.add_subscriber(client)

        Thread.new do

          
          begin

            loop do
              encrypted_message = client.gets
              if encrypted_message.nil?
                @broker.last_sent.delete(client)
                client.close
                @broker.subscribers.delete(client)
                break
              else
                decryptor = Kafkr::Encryptor.new
                message = decryptor.decrypt(encrypted_message.chomp) # Decrypt the message here
                uuid, message_content = extract_uuid(message)
                @broker.broadcast(JSON.dump(message_content))
              end
            rescue Errno::ECONNRESET
              client.close
            end 
          rescue StandardError => exception
            Kafkr.log "Error: #{exception.message}"
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
        Kafkr.log ">> #{message}"
        return message["uuid"], message
      rescue JSON::ParserError => e
        Kafkr.log ">> #{message}"
        match_data = /^(\w{8}-\w{4}-\w{4}-\w{4}-\w{12}): (.+)$/.match(message)
        match_data ? [match_data[1], match_data[2]] : [nil, nil]
      end
    end
  end
end
