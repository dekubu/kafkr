require "openssl"
require "base64"

module Kafkr
  class Encryptor
    ALGORITHM = 'AES-256-CBC'

    def initialize
      # Decoding the key from Base64 and ensuring it's the correct length for the algorithm
      @key = Base64.decode64("2wZ85yxQe0lmiQ5nsqdmPWoGB0W6HZW8S/UXVTLQ6WY=")
    end

    def encrypt(data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.encrypt
      cipher.key = @key
      iv = cipher.random_iv
      cipher.iv = iv

      # Encrypt the data
      encrypted_data = cipher.update(data) + cipher.final

      # Prepend the IV for use in decryption, then encode the result with Base64
      Base64.encode64(iv + encrypted_data)
    end

    def decrypt(encrypted_data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.decrypt
      cipher.key = @key

      # Decode the data from Base64
      decoded_data = Base64.decode64(encrypted_data)

      # Extract the IV and encrypted message
      iv = decoded_data[0...cipher.iv_len]
      encrypted_message = decoded_data[cipher.iv_len..-1]
      cipher.iv = iv

      # Decrypt the data
      cipher.update(encrypted_message) + cipher.final
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
