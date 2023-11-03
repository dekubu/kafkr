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
      iv = cipher.random_iv
      encrypted_data = cipher.update(data) + cipher.final
      encrypted_data = Base64.strict_encode64(iv + encrypted_data)
    end

    def decrypt(encrypted_data)
      puts "Encrypted data before decoding: #{encrypted_data.inspect}"
      decipher = OpenSSL::Cipher.new(ALGORITHM)
      decipher.decrypt
      decipher.key = @key
      raw_data = Base64.strict_decode64(encrypted_data)
      decipher.iv = raw_data[0, decipher.iv_len]
      decrypted_data = decipher.update(raw_data[decipher.iv_len..-1]) + decipher.final
      decrypted_data
    rescue OpenSSL::Cipher::CipherError => e
      puts "Decryption failed: #{e.message}"
      nil
    end
  end
end
