require "pry"

class WebHandler < Kafkr::Consumer::Handler
  def handle?(message)
    message.include?("web")
  end

  def handle(message)
    if message["sync"]
      reply to: message, payload: {test: "set"}
    end
  end
end
