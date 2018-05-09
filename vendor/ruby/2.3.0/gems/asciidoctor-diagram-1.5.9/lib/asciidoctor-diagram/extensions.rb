require 'asciidoctor' unless defined? ::Asciidoctor::VERSION
require 'asciidoctor/extensions'
require 'digest'
require 'json'
require 'fileutils'
require_relative 'version'
require_relative 'util/java'
require_relative 'util/gif'
require_relative 'util/pdf'
require_relative 'util/png'
require_relative 'util/svg'

module Asciidoctor
  module Diagram
    module Extensions
      # Provides the means for diagram processors to register supported output formats and image
      # generation routines
      module FormatRegistry
        # Registers a supported format. The first registered format becomes the default format for the block
        # processor.
        #
        # @param [Symbol] format the format name
        # @param [Symbol] type a symbol indicating the type of block that should be generated; either :image or :literal
        # @yieldparam parent [Asciidoctor::AbstractNode] the asciidoc block that is being processed
        # @yieldparam source [DiagramSource] the source object
        # @yieldreturn [String] the generated diagram
        #
        # Examples
        #
        #   register_format(:png, :image ) do |parent_block, source|
        #     File.read(source.to_s)
        #   end
        def register_format(format, type, &block)
          raise "Unsupported output type: #{type}" unless type == :image || type == :literal

          unless defined?(@default_format)
            @default_format = format
          end

          formats[format] = {
              :type => type,
              :generator => block
          }
        end

        # Returns the registered formats
        #
        # @return [Hash]
        # @api private
        def formats
          @formats ||= {}
        end

        # Returns the default format
        #
        # @return [Symbol] the default format
        # @api private
        def default_format
          @default_format
        end
      end

      # Mixin that provides the basic machinery for image generation.
      # When this module is included it will include the FormatRegistry into the singleton class of the target class.
      module DiagramProcessor
        IMAGE_PARAMS = {
            :svg => {
                :encoding => Encoding::UTF_8,
                :decoder => SVG
            },
            :gif => {
                :encoding => Encoding::ASCII_8BIT,
                :decoder => GIF
            },
            :png => {
                :encoding => Encoding::ASCII_8BIT,
                :decoder => PNG
            },
            :pdf => {
                :encoding => Encoding::ASCII_8BIT,
                :decoder => PDF
            }
        }

        def self.included(mod)
          class << mod
            include FormatRegistry
          end
        end

        # Processes the diagram block or block macro by converting it into an image or literal block.
        #
        # @param parent [Asciidoctor::AbstractBlock] the parent asciidoc block of the block or block macro being processed
        # @param reader_or_target [Asciidoctor::Reader, String] a reader that provides the contents of a block or the
        #        target value of a block macro
        # @param attributes [Hash] the attributes of the block or block macro
        # @return [Asciidoctor::AbstractBlock] a new block that replaces the original block or block macro
        def process(parent, reader_or_target, attributes)
          source = create_source(parent, reader_or_target, attributes.dup)

          format = source.attributes.delete('format') || source.attr('format', self.class.default_format, name)
          format = format.to_sym if format.respond_to?(:to_sym)

          raise "Format undefined" unless format

          generator_info = self.class.formats[format]

          raise "#{self.class.name} does not support output format #{format}" unless generator_info

          begin
            title = source.attributes.delete 'title'
            caption = source.attributes.delete 'caption'

            case generator_info[:type]
              when :literal
                block = create_literal_block(parent, source, generator_info)
              else
                block = create_image_block(parent, source, format, generator_info)
            end

            block.title = title
            block.assign_caption(caption, 'figure')
            block
          rescue => e
            case source.attr('on-error', 'log', 'diagram')
              when 'abort'
                raise e
              else
                text = "Failed to generate image: #{e.message}"
                warn_msg = text.dup
                if $VERBOSE
                  warn_msg << "\n" << e.backtrace.join("\n")
                end
                warn %(asciidoctor-diagram: ERROR: #{warn_msg})
                text << "\n"
                text << source.code
                Asciidoctor::Block.new parent, :listing, :source => text, :attributes => attributes
            end

          end
        end

        protected

        # Creates a DiagramSource object for the block or block macro being processed. Classes using this
        # mixin must implement this method.
        #
        # @param parent_block [Asciidoctor::AbstractBlock] the parent asciidoc block of the block or block macro being processed
        # @param reader_or_target [Asciidoctor::Reader, String] a reader that provides the contents of a block or the
        #        target value of a block macro
        # @param attributes [Hash] the attributes of the block or block macro
        #
        # @return [DiagramSource] an object that implements the interface described by DiagramSource
        #
        # @abstract
        def create_source(parent_block, reader_or_target, attributes)
          raise NotImplementedError.new
        end

        private
        DIGIT_CHAR_RANGE = ('0'.ord)..('9'.ord)

        def create_image_block(parent, source, format, generator_info)
          image_name = "#{source.image_name}.#{format}"
          image_dir = image_output_dir(parent)
          cache_dir = cache_dir(parent)
          image_file = parent.normalize_system_path image_name, image_dir
          metadata_file = parent.normalize_system_path "#{image_name}.cache", cache_dir

          if File.exist? metadata_file
            metadata = File.open(metadata_file, 'r') { |f| JSON.load f }
          else
            metadata = {}
          end

          image_attributes = source.attributes

          if !File.exist?(image_file) || source.should_process?(image_file, metadata)
            params = IMAGE_PARAMS[format]

            result = instance_exec(parent, source, &generator_info[:generator])

            result.force_encoding(params[:encoding])

            metadata = source.create_image_metadata
            metadata['width'], metadata['height'] = params[:decoder].get_image_size(result)

            FileUtils.mkdir_p(File.dirname(image_file)) unless Dir.exist?(File.dirname(image_file))
            File.open(image_file, 'wb') { |f| f.write result }

            FileUtils.mkdir_p(File.dirname(metadata_file)) unless Dir.exist?(File.dirname(metadata_file))
            File.open(metadata_file, 'w') { |f| JSON.dump(metadata, f) }
          end

          image_attributes['target'] = image_name

          scale = image_attributes['scale']
          if scalematch = /(\d+(?:\.\d+))/.match(scale)
            scale_factor = scalematch[1].to_f
          else
            scale_factor = 1.0
          end

          if /html/i =~ parent.document.attributes['backend']
            image_attributes.delete('scale')
            if metadata['width'] && !image_attributes['width']
              image_attributes['width'] = (metadata['width'] * scale_factor).to_i
            end
            if metadata['height'] && !image_attributes['height']
              image_attributes['height'] = (metadata['height'] * scale_factor).to_i
            end
          end

          image_attributes['alt'] ||= if title_text = image_attributes['title']
                                        title_text
                                      elsif target = image_attributes['target']
                                        (File.basename(target, File.extname(target)) || '').tr '_-', ' '
                                      else
                                        'Diagram'
                                      end

          image_attributes['alt'] = parent.sub_specialchars image_attributes['alt']

          parent.document.register(:images, image_name)
          if (scaledwidth = image_attributes['scaledwidth'])
            # append % to scaledwidth if ends in number (no units present)
            if DIGIT_CHAR_RANGE.include?((scaledwidth[-1] || 0).ord)
              image_attributes['scaledwidth'] = %(#{scaledwidth}%)
            end
          end

          Asciidoctor::Block.new parent, :image, :content_model => :empty, :attributes => image_attributes
        end

        def scale(size, factor)
          if match = /(\d+)(.*)/.match(size)
            value = match[1].to_i
            unit = match[2]
            (value * factor).to_i.to_s + unit
          else
            size
          end
        end

        def image_output_dir(parent)
          document = parent.document

          images_dir = parent.attr('imagesoutdir')

          if images_dir
            base_dir = nil
          else
            base_dir = parent.attr('outdir') || (document.respond_to?(:options) && document.options[:to_dir])
            images_dir = parent.attr('imagesdir')
          end

          parent.normalize_system_path(images_dir, base_dir)
        end

        def cache_dir(parent)
          document = parent.document
          cache_dir = '.asciidoctor/diagram'
          base_dir = parent.attr('outdir') || (document.respond_to?(:options) && document.options[:to_dir])
          parent.normalize_system_path(cache_dir, base_dir)
        end

        def create_literal_block(parent, source, generator_info)
          literal_attributes = source.attributes
          literal_attributes.delete('target')

          result = instance_exec(parent, source, &generator_info[:generator])

          result.force_encoding(Encoding::UTF_8)
          Asciidoctor::Block.new parent, :literal, :source => result, :attributes => literal_attributes
        end
      end

      # Base class for diagram block processors.
      class DiagramBlockProcessor < Asciidoctor::Extensions::BlockProcessor
        include DiagramProcessor

        def self.inherited(subclass)
          subclass.option :pos_attrs, ['target', 'format']
          subclass.option :contexts, [:listing, :literal, :open]
          subclass.option :content_model, :simple
        end

        # Creates a ReaderSource from the given reader.
        #
        # @return [ReaderSource] a ReaderSource
        def create_source(parent_block, reader, attributes)
          ReaderSource.new(parent_block, reader, attributes)
        end
      end

      # Base class for diagram block macro processors.
      class DiagramBlockMacroProcessor < Asciidoctor::Extensions::BlockMacroProcessor
        include DiagramProcessor

        def self.inherited(subclass)
          subclass.option :pos_attrs, ['target', 'format']
        end

        def apply_target_subs(parent, target)
          if target
            parent.normalize_system_path(parent.sub_attributes(target, :attribute_missing => 'warn'))
          else
            nil
          end
        end

        # Creates a FileSource using target as the file name.
        #
        # @return [FileSource] a FileSource
        def create_source(parent, target, attributes)
          FileSource.new(parent, apply_target_subs(parent, target), attributes)
        end
      end

      # This module describes the duck-typed interface that diagram sources must implement. Implementations
      # may include this module but it is not required.
      module DiagramSource
        # @return [String] the base name for the image file that will be produced
        # @abstract
        def image_name
          raise NotImplementedError.new
        end

        # @return [String] the String representation of the source code for the diagram
        # @abstract
        def code
          raise NotImplementedError.new
        end

        # Get the value for the specified attribute. First look in the attributes on
        # this node and return the value of the attribute if found. Otherwise, if
        # this node is a child of the Document node, look in the attributes of the
        # Document node and return the value of the attribute if found. Otherwise,
        # return the default value, which defaults to nil.
        #
        # @param name [String, Symbol] the name of the attribute to lookup
        # @param default_value [Object] the value to return if the attribute is not found
        # @inherit [Boolean, String] indicates whether to check for the attribute on the AsciiDoctor::Document if not found on this node.
        #                            When a non-nil String is given the an attribute name "#{inherit}-#{name}" is looked for on the document.
        #
        # @return the value of the attribute or the default value if the attribute is not found in the attributes of this node or the document node
        # @abstract
        def attr(name, default_value = nil, inherit = nil)
          raise NotImplementedError.new
        end

        # @return [String] the base directory against which relative paths in this diagram should be resolved
        # @abstract
        def base_dir
          attr('docdir', nil, true)
        end

        # Alias for code
        def to_s
          code
        end

        # Determines if the diagram should be regenerated or not. The default implementation of this method simply
        # returns true.
        #
        # @param image_file [String] the path to the previously generated version of the image
        # @param image_metadata [Hash] the image metadata Hash that was stored during the previous diagram generation pass
        # @return [Boolean] true if the diagram should be regenerated; false otherwise
        def should_process?(image_file, image_metadata)
          true
        end

        # Creates an image metadata Hash that will be stored to disk alongside the generated image file. The contents
        # of this Hash are reread during subsequent document processing and then passed to the should_process? method
        # where it can be used to determine if the diagram should be regenerated or not.
        # The default implementation returns an empty Hash.
        # @return [Hash] a Hash containing metadata
        def create_image_metadata
          {}
        end
      end

      # Base class for diagram source implementations that uses an md5 checksum of the source code of a diagram to
      # determine if it has been updated or not.
      class BasicSource
        include DiagramSource

        attr_reader :attributes

        def initialize(parent_block, attributes)
          @parent_block = parent_block
          @attributes = attributes
        end

        def image_name
          attr('target', 'diag-' + checksum)
        end

        def attr(name, default_value=nil, inherit=nil)
          name = name.to_s if ::Symbol === name

          value = @attributes[name]

          if value.nil? && inherit
            case inherit
              when String, Symbol
                value = @parent_block.attr("#{inherit.to_s}-#{name}", default_value, true)
              else
                value = @parent_block.attr(name, default_value, true)
            end
          end

          value || default_value
        end

        def should_process?(image_file, image_metadata)
          image_metadata['checksum'] != checksum
        end

        def create_image_metadata
          {'checksum' => checksum}
        end

        def checksum
          @checksum ||= compute_checksum(code)
        end

        protected
        def resolve_diagram_subs
          if @attributes.key? 'subs'
            @parent_block.resolve_block_subs @attributes['subs'], nil, 'diagram'
          else
            []
          end
        end

        private
        def compute_checksum(code)
          md5 = Digest::MD5.new
          md5 << code
          @attributes.each do |k, v|
            md5 << k.to_s if k
            md5 << v.to_s if v
          end
          md5.hexdigest
        end
      end

      # A diagram source that retrieves the code for the diagram from the contents of a block.
      class ReaderSource < BasicSource
        include DiagramSource

        def initialize(parent_block, reader, attributes)
          super(parent_block, attributes)
          @reader = reader
        end

        def code
          @code ||= @parent_block.apply_subs(@reader.lines, resolve_diagram_subs).join("\n")
        end
      end

      # A diagram source that retrieves the code for a diagram from an external source file.
      class FileSource < BasicSource
        def initialize(parent_block, file_name, attributes)
          super(parent_block, attributes)
          @file_name = file_name
        end

        def base_dir
          if @file_name
            File.dirname(@file_name)
          else
            super
          end
        end

        def image_name
          if @attributes['target']
            super
          elsif @file_name
            File.basename(@file_name, File.extname(@file_name))
          else
            checksum
          end
        end

        def should_process?(image_file, image_metadata)
          (@file_name && File.mtime(@file_name) > File.mtime(image_file)) || super
        end

        def code
          @code ||= read_code
        end

        def read_code
          if @file_name
            lines = File.readlines(@file_name)
            lines = ::Asciidoctor::Helpers.normalize_lines(lines)
            @parent_block.apply_subs(lines, resolve_diagram_subs).join("\n")
          else
            ''
          end
        end
      end
    end
  end
end
