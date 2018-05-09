require 'socket'

require_relative 'cli'
require_relative 'platform'
require_relative 'which'

module Asciidoctor
  module Diagram
    # @private
    module Java
      class CommandServer
        attr_reader :port

        def initialize(java, classpath)
          args = []
          args << '-Djava.awt.headless=true'
          args << '-cp'
          args << classpath.flatten.map { |jar| ::Asciidoctor::Diagram::Platform.host_os_path(jar).strip }.join(::Asciidoctor::Diagram::Platform.host_os_path_separator)
          args << 'org.asciidoctor.diagram.CommandServer'

          @server = IO.popen([java, *args])
          @port = @server.readline.strip.to_i
          @client = TCPSocket.new 'localhost', port
        end

        def io
          @client
        end

        def shutdown
          # KILL is a bit heavy handed, but TERM does not seem to shut down the JVM on Windows.
          Process.kill('KILL', @server.pid)
        end
      end

      def self.load
        if defined?(@loaded) && @loaded
          return
        end

        instance
        @loaded = true
      end

      def self.instance
        @java_exe ||= find_java
        raise "Could not find Java executable" unless @java_exe

        unless defined?(@command_server) && @command_server
          server = CommandServer.new(@java_exe, classpath)
          @command_server = server
          at_exit do
            server.shutdown
          end
        end

        @command_server
      end

      def self.send_request(req)
        svr = instance
        headers = req[:headers] ||= {}
        headers['Host'] = "localhost:#{svr.port}"
        format_request(req, svr.io)
        parse_response(svr.io)
      end

      private
      def self.find_java
        case ::Asciidoctor::Diagram::Platform.os
          when :windows
            path_to(ENV['JAVA_HOME'], 'bin/java.exe') || registry_lookup || ::Asciidoctor::Diagram::Which.which('java')
          when :macosx
            path_to(ENV['JAVA_HOME'], 'bin/java') || path_to(::Asciidoctor::Diagram::Cli.run('/usr/libexec/java_home')[:out].strip, 'bin/java') || ::Asciidoctor::Diagram::Which.which('java')
          else
            path_to(ENV['JAVA_HOME'], 'bin/java') || ::Asciidoctor::Diagram::Which.which('java')
        end
      end

      def self.path_to(java_home, java_binary)
        exe_path = File.expand_path(java_binary, java_home)
        if File.executable?(exe_path)
          exe_path
        else
          nil
        end
      end

      JDK_KEY = 'HKEY_LOCAL_MACHINE\SOFTWARE\JavaSoft\Java Development Kit'
      JRE_KEY = 'HKEY_LOCAL_MACHINE\SOFTWARE\JavaSoft\Java Runtime Environment'

      def self.registry_lookup
        registry_current(JRE_KEY) || registry_current(JDK_KEY) || registry_any()
      end

      def self.registry_current(key)
        current_version = registry_query(key, 'CurrentVersion')
        if current_version
          java_home = registry_query("#{key}\\#{current_version}", 'JavaHome')
          java_exe(java_home)
        else
          nil
        end
      end

      def self.registry_any()
        java_homes = registry_query('HKEY_LOCAL_MACHINE\SOFTWARE\JavaSoft', 'JavaHome', :recursive => true).values
        java_homes.map { |path| java_exe(path) }.find { |exe| !exe.nil? }
      end

      def self.java_exe(java_home)
        java = File.expand_path('bin/java.exe', java_home)

        if File.executable?(java)
          java
        else
          nil
        end
      end

      def self.registry_query(key, value = nil, opts = {})
        args = ['reg', 'query']
        args << key
        args << '/v' << value unless value.nil?
        args << '/s' if opts[:recursive]

        begin
          lines = ::Asciidoctor::Diagram::Cli.run(*args)[:out].lines.reject { |l| l.strip.empty? }.each
        rescue
          lines = [].each
        end

        result = {}

        while true
          begin
            begin
              k = lines.next
            rescue StopIteration
              break
            end

            unless k.start_with? key
              next
            end

            v = nil
            begin
              v = lines.next.strip if lines.peek.start_with?(' ')
            rescue StopIteration
              break
            end

            if !k.valid_encoding? || (v && !v.valid_encoding?)
              next
            end

            if v && (md = /([^\s]+)\s+(REG_[^\s]+)\s+(.+)/.match(v))
              v_name = md[1]
              v_value = md[3]
              result["#{k}\\#{v_name}"] = v_value
            else
              result[k] = v
            end
          end
        end

        if value && !opts[:recursive]
          result.values[0]
        else
          result
        end
      end
    end
  end
end