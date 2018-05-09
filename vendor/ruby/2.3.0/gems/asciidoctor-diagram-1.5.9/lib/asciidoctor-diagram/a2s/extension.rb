require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module AsciiToSvg
      include CliGenerator
      include Which

      def self.included(mod)
        [:svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            a2s(parent, source, f)
          end
        end
      end

      def a2s(parent, source, format)
        inherit_prefix = name

        sx = source.attr('scalex', nil, inherit_prefix)
        sy = source.attr('scaley', nil, inherit_prefix)
        scale = source.attr('scale', nil, inherit_prefix)
        noblur = source.attr('noblur', 'false', inherit_prefix) == 'true'
        font = source.attr('fontfamily', nil, inherit_prefix)

        generate_stdin(which(parent, 'a2s'), format.to_s, source.to_s) do |tool_path, output_path|
          args = [tool_path, "-o#{Platform.native_path(output_path)}"]

          if sx && sy
            args << "-s#{sx},#{sy}"
          elsif scale
            args << "-s#{scale},#{scale}"
          end

          if noblur
            args << '-b'
          end

          if font
            args << "-f#{font}"
          end

          args
        end
      end
    end

    class AsciiToSvgBlockProcessor < Extensions::DiagramBlockProcessor
      include AsciiToSvg
    end

    class AsciiToSvgBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include AsciiToSvg
    end
  end
end
