require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Umlet
      include CliGenerator
      include Which

      def self.included(mod)
        [:svg, :png, :pdf, :gif].each do |f|
          mod.register_format(f, :image) do |parent, source|
            umlet(parent, source, f)
          end
        end
      end

      def umlet(parent, source, format)
        generate_file(which(parent, 'umlet'), 'uxf', format.to_s, source.to_s) do |tool_path, input_path, output_path|
          [tool_path, '-action=convert', "-format=#{format.to_s}", "-filename=#{Platform.native_path(input_path)}", "-output=#{Platform.native_path(output_path)}"]
        end
      end
    end

    class UmletBlockProcessor < Extensions::DiagramBlockProcessor
      include Umlet
    end

    class UmletBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Umlet
    end
  end
end
