require "openssl"
require "base64"

module Kafkr
  class Encryptor
    ALGORITHM = "aes-256-cbc"

    def initialize
      @key = Base64.decode64(ENV["KAFKR_ENCRYPTION_KEY"])
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
      # Kafkr.log "Encrypted data before decoding: #{encrypted_data.inspect}"
      decipher = OpenSSL::Cipher.new(ALGORITHM)
      decipher.decrypt
      decipher.key = @key
      raw_data = Base64.strict_decode64(encrypted_data)
      decipher.iv = raw_data[0, decipher.iv_len]
      decipher.update(raw_data[decipher.iv_len..-1]) + decipher.final
    rescue OpenSSL::Cipher::CipherError => e
      Kafkr.log "Decryption failed: #{e.message}"
      nil
    end
  end
end
