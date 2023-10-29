require "socket"
require "rubygems"

module Kafkr
  class Log
    def initialize(port)
      @server = TCPServer.new(port)
      @received_file = "./.kafkr/log.txt"
      @broker = MessageBroker.new
      @whitelist = load_whitelist
      @acknowledged_message_ids = load_acknowledged_message_ids
    end

    def load_acknowledged_message_ids
      config_path = File.expand_path('./.kafkr/acknowledged_message_ids.txt')
      return [] unless File.exist?(config_path)

      File.readlines(config_path).map(&:strip)
    rescue Errno::ENOENT, Errno::EACCES => e
      puts "Error loading acknowledged_message_ids: #{e.message}"
      []
    end

    def start
      # Your existing 'start' method
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
  end
end
