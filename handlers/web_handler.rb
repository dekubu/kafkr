class WebHandler < Kafkr::Consumer::Handler
  def handle?(message)
    can_handle? message, "web"
  end

  def handle(message)
    puts "helo00"
    puts message
  end
end
