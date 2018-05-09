require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Wavedrom
      include CliGenerator
      include Which

      def self.included(mod)
        [:png, :svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            wavedrom(parent, source, f)
          end
        end
      end

      def wavedrom(parent, source, format)
        wavedrom_cli = which(parent, 'wavedrom', :raise_on_error => false)
        phantomjs = which(parent, 'phantomjs', :alt_attrs => ['phantomjs_2'], :raise_on_error => false)

        if wavedrom_cli && !wavedrom_cli.include?('WaveDromEditor') && phantomjs
          generate_file(wavedrom_cli, 'wvd', format.to_s, source.to_s) do |tool_path, input_path, output_path|
            [phantomjs, Platform.native_path(tool_path), '-i', Platform.native_path(input_path), "-#{format.to_s[0]}", Platform.native_path(output_path)]
          end
        else
          if ::Asciidoctor::Diagram::Platform.os == :macosx
            wavedrom = which(parent, 'WaveDromEditor.app', :alt_cmds => ['wavedrom-editor.app'], :attrs => ['WaveDromEditorApp'],:path => ['/Applications'])
            if wavedrom
              wavedrom = File.join(wavedrom, 'Contents/MacOS/nwjs')
            end
          else
            wavedrom = which(parent, 'WaveDromEditor')
          end

          generate_file(wavedrom, 'wvd', format.to_s, source.to_s) do |tool_path, input_path, output_path|
            [tool_path, 'source', Platform.native_path(input_path), format.to_s, Platform.native_path(output_path)]
          end
        end
      end
    end

    class WavedromBlockProcessor < Extensions::DiagramBlockProcessor
      include Wavedrom
    end

    class WavedromBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Wavedrom
    end
  end
end
