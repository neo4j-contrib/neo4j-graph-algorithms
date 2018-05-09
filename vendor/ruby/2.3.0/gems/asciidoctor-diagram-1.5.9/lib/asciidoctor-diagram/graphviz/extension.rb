require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Graphviz
      include CliGenerator
      include Which

      def self.included(mod)
        [:png, :pdf, :svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            graphviz(parent, source, f)
          end
        end
      end

      def graphviz(parent, source, format)
        inherit_prefix = name

        generate_stdin(which(parent, 'dot', :alt_attrs => ['graphvizdot']), format.to_s, source.to_s) do |tool_path, output_path|
          args = [tool_path, "-o#{Platform.native_path(output_path)}", "-T#{format.to_s}"]

          layout = source.attr('layout', nil, inherit_prefix)
          args << "-K#{layout}" if layout

          args
        end
      end
    end

    class GraphvizBlockProcessor < Extensions::DiagramBlockProcessor
      include Graphviz
    end

    class GraphvizBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Graphviz
    end
  end
end
