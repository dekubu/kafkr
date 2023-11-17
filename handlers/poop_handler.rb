class PoopHandler < Kafkr::Consumer::Handler
  def handle?(message)
    can_handle? message, 'poop'
  end

  def handle(message)
    puts message
  end
end
