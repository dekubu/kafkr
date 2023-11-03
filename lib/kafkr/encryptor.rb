require 'openssl'
require 'base64'

module Kafkr
  class Encryptor
    ALGORITHM = 'AES-256-CBC'
  
    def initialize
      @key = "lpN87qG7CWvmBpmHYcO1TjG1kD5jRxgbZGCS7/iWFas="
      @cipher = OpenSSL::Cipher.new(ALGORITHM)
    end
  
    def encrypt(data)
      @cipher.encrypt
      @cipher.key = @key
      iv = @cipher.random_iv

      encrypted_data = @cipher.update(data) + @cipher.final
      Base64.encode64(iv + encrypted_data)
    end
  
    def decrypt(encrypted_data)
      decoded_data = Base64.decode64(encrypted_data)
      iv = decoded_data[0..15]
      encrypted_message = decoded_data[16..-1]

      @cipher.decrypt
      @cipher.key = @key
      @cipher.iv = iv

      @cipher.update(encrypted_message) + @cipher.final
    end
  end
end
