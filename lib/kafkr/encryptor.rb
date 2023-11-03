module Kafkr
    class Encryptor
      ALGORITHM = 'AES-256-CBC'
  
      def initialize
        @key = ENV['KAFKR_KEY']
        @cipher = OpenSSL::Cipher.new(ALGORITHM)
      end
  
      def encrypt(data)
        @cipher.encrypt data
        @cipher.key = @key
        iv = @cipher.random_iv
  
        encrypted_data = @cipher.update(data) + @cipher.final
        "#{iv.unpack1('m')}--#{encrypted_data.unpack1('m')}"
      end
  
      def decrypt(encrypted_data)
        iv, encrypted_message = encrypted_data.split('--').map { |part| part.unpack1('m') }
        @cipher.decrypt
        @cipher.key = @key
        @cipher.iv = iv
        @cipher.update(encrypted_message) + @cipher.final
      end
    end
end