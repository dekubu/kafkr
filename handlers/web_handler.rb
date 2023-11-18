require "debug"
class WebHandler < Kafkr::Consumer::Handler
  def handle?(message)
    can_handle? message, "web"
  end

  def handle(message)
    puts message
    if message["sync"]
      reply to: message, payload: {test: "set"}
    end
  end
end
