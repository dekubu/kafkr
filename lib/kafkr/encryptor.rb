require "openssl"
require "base64"

module Kafkr
  class Encryptor
    ALGORITHM = 'AES-256-CBC'
  
    def initialize
      @key = Base64.decode64("2wZ85yxQe0lmiQ5nsqdmPWoGB0W6HZW8S/UXVTLQ6WY=") 
      @cipher = OpenSSL::Cipher.new(ALGORITHM)
    end
  
    def encrypt(data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.encrypt
      cipher.key = @key
      iv = cipher.random_iv

      encrypted_data = cipher.update(data) + cipher.final
      Base64.encode64(iv + encrypted_data)
    end

    def decrypt(encrypted_data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.decrypt
      cipher.key = @key

      decoded_data = Base64.decode64(encrypted_data)

      # The IV is exactly the first 16 bytes of the decoded data
      cipher.iv = decoded_data[0, cipher.iv_len]

      # The encrypted message is everything after the IV
      encrypted_message = decoded_data[cipher.iv_len..]

      # Decrypt and return the plaintext message
      cipher.update(encrypted_message) + cipher.final
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
