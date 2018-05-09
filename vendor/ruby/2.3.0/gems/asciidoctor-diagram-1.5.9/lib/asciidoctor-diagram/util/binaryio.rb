module Asciidoctor
  module Diagram
    # Helper class to read binary values from a String
    # @private
    class BinaryIO
      def initialize(string)
        @data = string
        @offset = 0
      end

      def read_uint16_be
        uint16 = @data[@offset,2].unpack('n')[0]
        @offset += 2
        uint16
      end

      def read_uint16_le
        uint16 = @data[@offset,2].unpack('v')[0]
        @offset += 2
        uint16
      end

      def read_uint32_be
        uint32 = @data[@offset,4].unpack('N')[0]
        @offset += 4
        uint32
      end

      def read_uint32_le
        uint32 = @data[@offset,4].unpack('V')[0]
        @offset += 4
        uint32
      end

      def read_string(length, encoding = Encoding::ASCII_8BIT)
        str = @data[@offset,length]
        @offset += length
        str.force_encoding(encoding)
      end

      def skip(length)
        @offset += length
      end
    end
  end
end