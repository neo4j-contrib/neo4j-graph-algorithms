require_relative 'binaryio'

module Asciidoctor
  module Diagram
    # @private
    module SVG
      def self.get_image_size(data)
        if m = START_TAG_REGEX.match(data)
          start_tag = m[0]
          if (w = WIDTH_REGEX.match(start_tag)) && (h = HEIGHT_REGEX.match(start_tag))
            width = w[:value].to_i * to_px_factor(w[:unit])
            height = h[:value].to_i * to_px_factor(h[:unit])
            return [width.to_i, height.to_i]
          end

          if v = VIEWBOX_REGEX.match(start_tag)
            width = v[:width]
            height = v[:height]
            return [width.to_i, height.to_i]
          end
        end

        nil
      end

      private

      START_TAG_REGEX = /<svg[^>]*>/
      WIDTH_REGEX = /width="(?<value>\d+(?:\.\d+)?)(?<unit>[a-zA-Z]+)?"/
      HEIGHT_REGEX = /height="(?<value>\d+(?:\.\d+)?)(?<unit>[a-zA-Z]+)?"/
      VIEWBOX_REGEX = /viewBox="\d+(?:\.\d+)? \d+(?:\.\d+)? (?<width>\d+(?:\.\d+)?) (?<height>\d+(?:\.\d+)?)"/

      def self.to_px_factor(unit)
        case unit
          when 'pt'
            1.33
          else
            1
        end
      end
    end
  end
end