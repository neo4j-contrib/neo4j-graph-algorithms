require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/platform'
require_relative '../util/which'

module Asciidoctor
  module Diagram
    # @!parse
    #   # Block processor converts blockdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class BlockDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts blockdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class BlockDiagBlockMacroProcessor < DiagramBlockMacroProcessor; end

    # @!parse
    #   # Block processor converts seqdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class SeqDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts seqdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class SeqDiagBlockMacroProcessor < API::DiagramBlockMacroProcessor; end

    # @!parse
    #   # Block processor converts actdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class ActDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts actdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class ActDiagBlockMacroProcessor < API::DiagramBlockMacroProcessor; end

    # @!parse
    #   # Block processor converts nwdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class NwDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts nwdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class NwDiagBlockMacroProcessor < API::DiagramBlockMacroProcessor; end

    # @!parse
    #   # Block processor converts rackdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class RackDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts rackdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class RackDiagBlockMacroProcessor < API::DiagramBlockMacroProcessor; end

    # @!parse
    #   # Block processor converts packetdiag code into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class PacketDiagBlockProcessor < API::DiagramBlockProcessor; end
    #
    #   # Block macro processor converts packetdiag source files into images.
    #   #
    #   # Supports PNG and SVG output.
    #   class PacketDiagBlockMacroProcessor < API::DiagramBlockMacroProcessor; end

    # @private
    module BlockDiag
      def self.define_processors(name)
        init = Proc.new do
          include ::Asciidoctor::Diagram::BlockDiag

          [:png, :pdf, :svg].each do |f|
            register_format(f, :image) do |p, c|
              blockdiag(name, p, c, f)
            end
          end
        end

        block = Class.new(Extensions::DiagramBlockProcessor) do
          self.instance_eval(&init)
        end
        ::Asciidoctor::Diagram.const_set("#{name}BlockProcessor", block)

        block_macro = Class.new(Extensions::DiagramBlockMacroProcessor) do
          self.instance_eval(&init)
        end

        ::Asciidoctor::Diagram.const_set("#{name}BlockMacroProcessor", block_macro)
      end

      include CliGenerator
      include Which

      def blockdiag(tool, parent, source, format)
        inherit_prefix = name
        cmd_name = tool.downcase

        # On Debian based systems the Python 3.x packages python3-(act|block|nw|seq)diag executables with
        # a '3' suffix.
        alt_cmd_name = "#{tool.downcase}3"

        font_path = source.attr('fontpath', nil, inherit_prefix)

        generate_stdin(which(parent, cmd_name, :alt_cmds => [alt_cmd_name]), format.to_s, source.to_s) do |tool_path, output_path|
          args = [tool_path, '-a', '-o', Platform.native_path(output_path), "-T#{format.to_s}"]
          args << "-f#{Platform.native_path(font_path)}" if font_path
          args << '-'
          args
        end
      end
    end

    ['BlockDiag', 'SeqDiag', 'ActDiag', 'NwDiag', 'RackDiag', 'PacketDiag'].each do |tool|
      BlockDiag.define_processors(tool)
    end
  end
end
