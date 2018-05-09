require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Svgbob
      include CliGenerator
      include Which

      def self.included(mod)
        [:svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            svgbob(parent, source, f)
          end
        end
      end

      def svgbob(parent, source, format)
        generate_stdin(which(parent, 'svgbob'), format.to_s, source.to_s) do |tool_path, output_path|
          [tool_path, '-o', Platform.native_path(output_path)]
        end
      end
    end

    class SvgBobBlockProcessor < Extensions::DiagramBlockProcessor
      include Svgbob
    end

    class SvgBobBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Svgbob
    end
  end
end
