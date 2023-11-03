require "openssl"
require "base64"

module Kafkr
  class Encryptor
    ALGORITHM = 'aes-256-cbc'

    def initialize
      @key = Base64.decode64("2wZ85yxQe0lmiQ5nsqdmPWoGB0W6HZW8S/UXVTLQ6WY=")
    end

    def encrypt(data)
      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.encrypt
      cipher.key = @key
      encrypted_data = cipher.update(data) + cipher.final
      Base64.encode64(encrypted_data).gsub("\n", '')
    end

    def decrypt(encrypted_data)
      decipher = OpenSSL::Cipher.new(ALGORITHM)
      decipher.decrypt
      decipher.key = @key
      decrypted_data = decipher.update(Base64.decode64(encrypted_data)) + decipher.final
      decrypted_data
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
