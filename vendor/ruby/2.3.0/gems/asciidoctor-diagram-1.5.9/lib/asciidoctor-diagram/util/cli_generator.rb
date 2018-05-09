require 'tempfile'
require_relative 'cli'

module Asciidoctor
  module Diagram
    # @private
    module CliGenerator
      def generate_stdin(tool, format, code)
        tool_name = File.basename(tool)

        target_file = Tempfile.new([tool_name, ".#{format}"])
        begin
          target_file.close

          opts = yield tool, target_file.path

          generate(opts, target_file.path, :stdin_data => code)
        ensure
          target_file.unlink
        end
      end

      def generate_stdin_stdout(tool, code)
        if block_given?
          opts = yield tool
        else
          opts = [tool]
        end
        generate(opts, :stdout, :stdin_data => code)
      end

      def generate_file(tool, input_ext, output_ext, code)
        tool_name = File.basename(tool)

        source_file = Tempfile.new([tool_name, ".#{input_ext}"])
        begin
          File.write(source_file.path, code)

          target_file = Tempfile.new([tool_name, ".#{output_ext}"])
          begin
            target_file.close

            opts = yield tool, source_file.path, target_file.path

            generate(opts, target_file.path)
          ensure
            target_file.unlink
          end
        ensure
          source_file.unlink
        end
      end

      private
      def generate(opts, target_file, open3_opts = {})
        case opts
          when Array
            args = opts
            out_file = nil
          when Hash
            args = opts[:args]
            out_file = opts[:out_file]
          else
            raise "Block passed to generate_file should return an Array or a Hash"
        end

        result = ::Asciidoctor::Diagram::Cli.run(*args, open3_opts)

        data = target_file == :stdout ? result[:out] : read_result(target_file, out_file)

        if data.empty?
          raise "#{args[0]} failed: #{result[:out].empty? ? result[:err] : result[:out]}"
        end

        data
      end

      def read_result(target_file, out_file = nil)
        if File.exist?(out_file || target_file)
          if out_file
            File.rename(out_file, target_file)
          end

          File.binread(target_file)
        else
          ''
        end
      end
    end
  end
end
