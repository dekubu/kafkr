require "openssl"
require "base64"

module Kafkr
  class Encryptor
    
    attr_reader :key, :cipher

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
      decoded_data = Base64.decode64(encrypted_data)
    
      # Verify the IV length is correct
      unless decoded_data.length >= cipher.iv_len
        puts "Invalid data: IV length is too short"
        return nil
      end
    
      cipher.iv = decoded_data[0, cipher.iv_len]
      encrypted_message = decoded_data[cipher.iv_len..]
    
      # Verify the encrypted message isn't empty
      if encrypted_message.nil? || encrypted_message.empty?
        puts "Invalid data: Encrypted message is missing"
        return nil
      end
    
      cipher.update(encrypted_message) + cipher.final
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
    
  end
end
