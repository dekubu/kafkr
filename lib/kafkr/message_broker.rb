module Kafkr
class MessageBroker
    attr_accessor :last_sent,:subscribers

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
      @subscribers.each do |subscriber|
        begin
          if !subscriber.closed?
            begin
              subscriber.puts(message)
              @last_sent[subscriber] = message
            rescue Errno::EPIPE
              
            end
          end
        rescue IOError
          @subscribers.delete(subscriber)
          @last_sent.delete(subscriber)
        end
      end
    end
  end
end