require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Syntrax
      include CliGenerator
      include Which

      def self.included(mod)
        [:png, :svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            syntrax(parent, source, f)
          end
        end
      end

      def syntrax(parent, source, format)
        inherit_prefix = name

        generate_file(which(parent, 'syntrax'), 'spec', format.to_s, source.to_s) do |tool_path, input_path, output_path|
          args = [tool_path, '-i', Platform.native_path(input_path), '-o', Platform.native_path(output_path)]

          title = source.attr('heading', nil, inherit_prefix)
          if title
            args << '--title' << title
          end

          scale = source.attr('scale', nil, inherit_prefix)
          if scale
            args << '--scale' << scale
          end

          transparent = source.attr('transparent', nil, inherit_prefix)
          if transparent == 'true'
            args << '--transparent'
          end
          style = source.attr('style', nil, inherit_prefix)
          if style
            args << '--style' << style
          end

          args
        end
      end
    end

    class SyntraxBlockProcessor < Extensions::DiagramBlockProcessor
      include Syntrax
    end

    class SyntraxBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Syntrax
    end
  end
end
