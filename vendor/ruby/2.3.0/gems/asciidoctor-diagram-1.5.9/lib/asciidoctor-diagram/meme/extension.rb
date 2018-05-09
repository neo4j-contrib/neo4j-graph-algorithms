require_relative '../extensions'
require_relative '../util/cli_generator'
require_relative '../util/which'
require 'tempfile'
require 'open3'

module Asciidoctor
  module Diagram
    # @private
    module Meme
      include Which

      def self.included(mod)
        [:png, :gif].each do |format|
          mod.register_format(format, :image) do |parent, source|
            meme(parent, source, format)
          end
        end
      end

      def meme(parent_block, source, format)
        convert = which(parent_block, 'convert')
        identify = which(parent_block, 'identify')
        inherit_prefix = name

        bg_img = source.attr('background', nil, inherit_prefix)
        raise "background attribute is required" unless bg_img

        bg_img = parent_block.normalize_system_path(bg_img, parent_block.attr('imagesdir'))

        top_label = source.attr('top')
        bottom_label = source.attr('bottom')
        fill_color = source.attr('fillColor', 'white', inherit_prefix)
        stroke_color = source.attr('strokeColor', 'black', inherit_prefix)
        stroke_width = source.attr('strokeWidth', '2', inherit_prefix)
        font = source.attr('font', 'Impact', inherit_prefix)
        options = source.attr('options', '', inherit_prefix).split(',')
        noupcase = options.include?('noupcase')

        dimensions = Cli.run(identify, '-format', '%w %h', bg_img)[:out].match(/(?<w>\d+) (?<h>\d+)/)
        bg_width = dimensions['w'].to_i
        bg_height = dimensions['h'].to_i
        label_width = bg_width
        label_height = bg_height / 5

        if top_label
          top_img = Tempfile.new(['meme', '.png'])
          Cli.run(
              convert,
              '-background', 'none',
              '-fill', fill_color,
              '-stroke', stroke_color,
              '-strokewidth', stroke_width,
              '-font', font,
              '-size', "#{label_width}x#{label_height}",
              '-gravity', 'north',
              "label:#{prepare_label(top_label, noupcase)}",
              top_img.path
          )
        else
          top_img = nil
        end

        if bottom_label
          bottom_img = Tempfile.new(['meme', '.png'])
          Cli.run(
              convert,
              '-background', 'none',
              '-fill', fill_color,
              '-stroke', stroke_color,
              '-strokewidth', stroke_width,
              '-font', font,
              '-size', "#{label_width}x#{label_height}",
              '-gravity', 'south',
              "label:#{prepare_label(bottom_label, noupcase)}",
              bottom_img.path
          )
        else
          bottom_img = nil
        end

        final_img = Tempfile.new(['meme', ".#{format.to_s}"])

        args = [convert, bg_img]
        if top_img
          args << top_img.path << '-geometry'<< '+0+0' << '-composite'
        end

        if bottom_img
          args << bottom_img.path << '-geometry'<< "+0+#{bg_height - label_height}" << '-composite'
        end

        args << final_img.path

        Cli.run(*args)

        File.binread(final_img)
      end

      private
      def prepare_label(label, noupcase)
        label = label.upcase unless noupcase
        label = label.gsub(' // ', '\n')
        label
      end
    end

    class MemeBlockMacroProcessor < Extensions::DiagramBlockMacroProcessor
      include Meme

      class StringReader
        def initialize(str)
          @str = str
        end

        def lines
          @str.lines.map { |l| l.rstrip }
        end
      end

      option :pos_attrs, %w(top bottom target format)

      def create_source(parent, target, attributes)
        attributes = attributes.dup
        attributes['background'] = apply_target_subs(parent, target)
        ::Asciidoctor::Diagram::Extensions::ReaderSource.new(parent, StringReader.new(''), attributes)
      end
    end
  end
end
