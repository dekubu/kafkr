module Kafkr
  class MessageBroker
    attr_accessor :last_sent, :subscribers

    def initialize
      @subscribers = []
      @last_sent = {}
    end

    def add_subscriber(socket)
      @subscribers << socket
      @last_sent[socket] = nil
    end

    def broadcast(message)
      Kafkr.log message

      encrypted_message = Kafkr::Encryptor.new.encrypt(message)

      @subscribers.each do |subscriber|
        if !subscriber.closed?
          subscriber.puts(encrypted_message)
          @last_sent[subscriber] = encrypted_message
        end
      rescue Errno::EPIPE
        # Optionally, handle broken pipe error
      rescue IOError
        @subscribers.delete(subscriber)
        @last_sent.delete(subscriber)
      end
    end
  end
end
