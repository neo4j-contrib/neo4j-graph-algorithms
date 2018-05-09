module Asciidoctor
  module Diagram
    module Which
      # @private
      def self.which(cmd, options = {})
        exts = ENV['PATHEXT'] ? ENV['PATHEXT'].split(';') : ['']

        paths = (options[:path] || []) + (ENV['PATH'] ? ENV['PATH'].split(File::PATH_SEPARATOR) : [])
        paths.each do |path|
          exts.each { |ext|
            exe = File.join(path, "#{cmd}#{ext}")
            return exe if File.executable? exe
          }
        end

        nil
      end

      def which(parent_block, cmd, options = {})
        attr_names = options[:attrs] || options.fetch(:alt_attrs, []) + [cmd]
        cmd_names = [cmd] + options.fetch(:alt_cmds, [])

        cmd_var = 'cmd-' + attr_names[0]

        if config.key? cmd_var
          cmd_path = config[cmd_var]
        else
          cmd_path = attr_names.map { |attr_name| parent_block.attr(attr_name, nil, true) }.find { |attr| !attr.nil? }

          unless cmd_path && File.executable?(cmd_path)
            cmd_paths = cmd_names.map do |c|
              ::Asciidoctor::Diagram::Which.which(c, :path => options[:path])
            end

            cmd_path = cmd_paths.reject { |c| c.nil? }.first
          end

          config[cmd_var] = cmd_path

          if cmd_path.nil? && options.fetch(:raise_on_error, true)
            raise "Could not find the #{cmd_names.map { |c| "'#{c}'" }.join(', ')} executable in PATH; add it to the PATH or specify its location using the '#{attr_names[0]}' document attribute"
          end
        end

        cmd_path
      end
    end
  end
end
