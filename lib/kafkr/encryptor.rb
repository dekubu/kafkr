require "openssl"
require "base64"

module Kafkr
  class Encryptor
    ALGORITHM = "AES-256-CBC"

    attr_reader :key, :cipher

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
      Base64.encode64(iv + encrypted_data) # Prepend the IV to the encrypted data
    end

    def decrypt(encrypted_data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.decrypt
      cipher.key = @key

      decoded_data = Base64.decode64(encrypted_data)

      # Extract the IV from the beginning of the decoded data
      iv = decoded_data[0, cipher.iv_len]
      cipher.iv = iv

      # Extract the encrypted message, which is everything after the IV
      encrypted_message = decoded_data[cipher.iv_len..-1]

      # Decrypt and return the plaintext message
      cipher.update(encrypted_message) + cipher.final
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
