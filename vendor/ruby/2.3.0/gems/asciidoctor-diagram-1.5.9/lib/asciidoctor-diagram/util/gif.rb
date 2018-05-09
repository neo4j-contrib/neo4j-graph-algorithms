require_relative 'binaryio'

module Asciidoctor
  module Diagram
    # @private
    module GIF
      GIF87A_SIGNATURE = 'GIF87a'.force_encoding(Encoding::ASCII_8BIT)
      GIF89A_SIGNATURE = 'GIF89a'.force_encoding(Encoding::ASCII_8BIT)

      def self.get_image_size(data)
        bio = BinaryIO.new(data)
        gif_signature = bio.read_string(6)
        raise "Invalid GIF signature" unless gif_signature == GIF87A_SIGNATURE || gif_signature == GIF89A_SIGNATURE

        width = bio.read_uint16_le
        height = bio.read_uint16_le
        [width, height]
      end
    end
  end
end