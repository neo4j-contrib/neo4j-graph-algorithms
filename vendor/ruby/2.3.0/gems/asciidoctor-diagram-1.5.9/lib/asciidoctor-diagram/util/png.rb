require_relative 'binaryio'

module Asciidoctor
  module Diagram
    # @private
    module PNG
      PNG_SIGNATURE = [137, 80, 78, 71, 13, 10, 26, 10].pack('CCCCCCCC')

      def self.get_image_size(data)
        bio = BinaryIO.new(data)
        png_signature = bio.read_string(8)
        raise "Invalid PNG signature" unless png_signature == PNG_SIGNATURE

        chunk_length = bio.read_uint32_be
        chunk_type = bio.read_string(4, Encoding::US_ASCII)
        raise "Unexpected PNG chunk type '#{chunk_type}'; expected 'IHDR'" unless chunk_type == 'IHDR'
        raise "Unexpected PNG chunk length '#{chunk_length}'; expected '13'" unless chunk_length == 13

        width = bio.read_uint32_be
        height = bio.read_uint32_be
        [width, height]
      end
    end
  end
end