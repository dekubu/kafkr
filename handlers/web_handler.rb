require 'debug'
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
  def reply to: , payload:
    Kafkr::Producer.send_message("reply => #{payload}, sync_uid: #{to['sync_uid']}")
  end
end
