require_relative '../extensions'
require_relative '../util/cli'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @private
    module Mermaid
      include CliGenerator
      include Which

      def self.included(mod)
        [:png, :svg].each do |f|
          mod.register_format(f, :image) do |parent, source|
            mermaid(parent, source, f)
          end
        end
      end

      def mermaid(parent_block, source, format)
        inherit_prefix = name

        options = {}

        css = source.attr('css', nil, inherit_prefix)
        if css
          options[:css] = parent_block.normalize_system_path(css, source.base_dir)
        end

        gantt_config = source.attr('ganttConfig', nil, inherit_prefix) || source.attr('ganttconfig', nil, inherit_prefix)
        if gantt_config
          options[:gantt] = parent_block.normalize_system_path(gantt_config, source.base_dir)
        end

        seq_config = source.attr('sequenceConfig', nil, inherit_prefix) || source.attr('sequenceconfig', nil, inherit_prefix)
        if seq_config
          options[:sequence] = parent_block.normalize_system_path(seq_config, source.base_dir)
        end

        options[:width] = source.attr('width', nil, inherit_prefix)

        mmdc = which(parent_block, 'mmdc', :raise_on_error => false)
        if mmdc
          options[:height] = source.attr('height', nil, inherit_prefix)
          options[:theme] = source.attr('theme', nil, inherit_prefix)
          options[:background] = source.attr('background', nil, inherit_prefix)
          config = source.attr('config', nil, inherit_prefix) || source.attr('config', nil, inherit_prefix)
          if config
            options[:config] = parent_block.normalize_system_path(config, source.base_dir)
          end
          run_mmdc(mmdc, source, format, options)
        else
          mermaid = which(parent_block, 'mermaid')
          run_mermaid(mermaid, parent_block, source, format, options)
        end
      end

      private
      def run_mmdc(mmdc, source, format, options = {})
        generate_file(mmdc, 'mmd', format.to_s, source.to_s) do |tool_path, input_path, output_path|
          args = [tool_path, '-i', Platform.native_path(input_path), '-o', Platform.native_path(output_path)]

          if options[:css]
            args << '--cssFile' << Platform.native_path(options[:css])
          end

          if options[:theme]
            args << '--theme' << options[:theme]
          end

          if options[:width]
            args << '--width' << options[:width]
          end

          if options[:height]
            args << '--height' << options[:height]
          end

          if options[:background]
            bg = options[:background]
            bg = "##{bg}" unless bg[0] == '#'
            args << '--backgroundColor' << bg
          end

          if options[:config]
            args << '--configFile' << Platform.native_path(options[:config])
          elsif options[:gantt] || options[:sequence]
            mermaidConfig = []

            if options[:gantt]
              mermaidConfig << "\"gantt\": #{File.read(options[:gantt])}"
            end

            if options[:sequence]
              configKey = config['mmdcSequenceConfigKey'] ||= begin
                version_parts = ::Asciidoctor::Diagram::Cli.run(mmdc, '--version')[:out].split('.').map { |p| p.to_i }
                major = version_parts[0] || 0
                minor = version_parts[1] || 0
                patch = version_parts[2] || 0
                if major > 0 || (major == 0 && minor > 4) || (major == 0 && minor == 4 && patch > 1)
                  'sequence'
                else
                  'sequenceDiagram'
                end
              end
              mermaidConfig << "\"#{configKey}\": #{File.read(options[:sequence])}"
            end

            config_file = "#{input_path}.json"

            File.write(config_file, "{#{mermaidConfig.join ','}}")

            args << '--configFile' << Platform.native_path(config_file)
          end

          args
        end
      end

      def run_mermaid(mermaid, parent_block, source, format, options = {})
        config['mermaid>=6'] ||= ::Asciidoctor::Diagram::Cli.run(mermaid, '--version')[:out].split('.')[0].to_i >= 6
        # Mermaid >= 6.0.0 requires PhantomJS 2.1; older version required 1.9
        phantomjs = which(parent_block, 'phantomjs', :alt_attrs => [config['mermaid>=6'] ? 'phantomjs_2' : 'phantomjs_19'])

        generate_file(mermaid, 'mmd', format.to_s, source.to_s) do |tool_path, input_path, output_path|
          output_dir = File.dirname(output_path)
          output_file = File.expand_path(File.basename(input_path) + ".#{format.to_s}", output_dir)

          args = [tool_path, '--phantomPath', Platform.native_path(phantomjs), "--#{format.to_s}", '-o', Platform.native_path(output_dir)]

          if options[:css]
            args << '--css' << Platform.native_path(options[:css])
          end

          if options[:gantt]
            args << '--gantt_config' << Platform.native_path(options[:gantt])
          end

          if options[:sequence]
            args << '--sequenceConfig' << Platform.native_path(options[:sequence])
          end

          if options[:width]
            args << '--width' << options[:width]
          end

          args << Platform.native_path(input_path)

          {
              :args => args,
              :out_file => output_file
          }
        end
      end
    end

    class MermaidBlockProcessor < Extensions::DiagramBlockProcessor
      include Mermaid
    end

    class MermaidBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Mermaid
    end
  end
end
