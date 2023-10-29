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
      log message
      @subscribers.each do |subscriber|
        begin
          if !subscriber.closed?
            subscriber.puts(message)
            @last_sent[subscriber] = message
          end
        rescue IOError
          @subscribers.delete(subscriber)
          @last_sent.delete(subscriber)
        end
      end
    end
  end
end