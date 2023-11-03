require "openssl"
require "base64"

module Kafkr
  class Encryptor
    attr_reader :cipher
    def initialize
      @cipher = Gibberish::AES.new(Base64.decode64("2wZ85yxQe0lmiQ5nsqdmPWoGB0W6HZW8S/UXVTLQ6WY="))  
    end

    def encrypt(data)
      Base64.encode(cipher.encrypt(data))
    end

    def decrypt(data)
      cipher.decrypt(Base64.decode64(cipher.encrypt(data)))
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
