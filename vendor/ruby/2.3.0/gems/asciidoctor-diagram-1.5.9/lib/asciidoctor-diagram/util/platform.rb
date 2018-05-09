require 'rbconfig'
require_relative 'cli'
require_relative 'which'

module Asciidoctor
  module Diagram
    module Platform
      def self.os
        os_info[:os]
      end

      def self.os_variant
        os_info[:os_variant]
      end

      def self.file_separator
        os_info[:file_sep]
      end

      def self.host_os_file_separator
        os_info[:host_os_file_sep]
      end

      def self.path_separator
        os_info[:path_sep]
      end

      def self.host_os_path_separator
        os_info[:host_os_path_sep]
      end
      
      def self.os_info
        @os ||= (
        host_os = RbConfig::CONFIG['host_os']

        file_sep = '/'
        host_os_file_sep = nil
        variant = nil
        path_sep = ::File::PATH_SEPARATOR
        host_os_path_sep = nil
        
        case host_os
          when /(msys|mingw|mswin|bccwin|wince|emc)/i
            os = :windows
            variant = $1.downcase.to_sym
            file_sep = '\\'
          when /(cygwin)/i
            os = :windows
            variant = $1.downcase.to_sym
            host_os_file_sep = '\\'
            host_os_path_sep = ';'
          when /darwin|mac os/i
            os = :macosx
          when /linux/i
            os = :linux
          else
            os = :unix
        end
        {
            :os => os,
            :os_variant => variant || os, 
            :file_sep => file_sep,
            :host_os_file_sep => host_os_file_sep || file_sep,
            :path_sep => path_sep,
            :host_os_path_sep => host_os_path_sep || path_sep
        }
        )
      end

      def self.native_path(path)
        return path if path.nil?

        sep = file_separator
        if sep != '/'
          path.to_s.gsub('/', sep)
        else
          path.to_s
        end
      end

      def self.host_os_path(path)
        # special case for cygwin, it requires path translation for java to work
        if os_variant == :cygwin
          cygpath = ::Asciidoctor::Diagram::Which.which('cygpath')
          if cygpath != nil
            ::Asciidoctor::Diagram::Cli.run(cygpath, '-w', path)[:out]
          else
            puts 'cygwin warning: cygpath not found'
            native_path(path)
          end
        else
          native_path(path)
        end
      end
    end
  end
end
