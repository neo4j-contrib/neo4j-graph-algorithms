require 'json'

module Asciidoctor
  module Diagram
    # @private
    module Java
      def self.classpath
        @classpath ||= [
            File.expand_path(File.join('../..', 'server-1.3.13.jar'), File.dirname(__FILE__))
        ]
      end

      CRLF = "\r\n".encode(Encoding::US_ASCII)

      def self.format_request(req, io)
        io.set_encoding Encoding::US_ASCII
        io.write "POST #{req[:url]} HTTP/1.1"
        io.write CRLF

        headers = req[:headers]
        if headers
          headers.each_pair do |key, value|
            io.write "#{key}: #{value}"
            io.write CRLF
          end
        end

        if req[:body]
          unless headers && headers['Content-Length']
            io.write 'Content-Length: '
            io.write req[:body].bytesize.to_s
            io.write CRLF
          end

          unless headers && headers['Content-Type']
            io.write 'Content-Type: text/plain; charset='
            io.write req[:body].encoding.name
            io.write CRLF
          end
        end

        io.write CRLF

        io.set_encoding Encoding::BINARY
        io.write req[:body]
      end

      STATUS_LINE = Regexp.new("HTTP/1.1 (\\d+) (.*)\r\n".encode(Encoding::US_ASCII))

      def self.parse_response(io)
        resp = {}

        io.set_encoding Encoding::US_ASCII
        status_line = io.readline(CRLF)
        status_line_parts = STATUS_LINE.match status_line
        resp[:code] = status_line_parts[1].to_i
        resp[:reason] = status_line_parts[2]

        headers = {}
        until (header = io.readline(CRLF).strip).empty?
          key, value = header.split ':', 2
          headers[key] = value.strip
        end

        resp[:headers] = headers

        content_length = headers['Content-Length']
        if content_length
          io.set_encoding Encoding::BINARY
          resp[:body] = io.read(content_length.to_i)
        end

        resp
      end

      def self.create_error(prefix_msg, response)
        content_type = response[:headers]['Content-Type'] || 'text/plain'
        if content_type.start_with? 'application/json'
          json = JSON.parse(response[:body].force_encoding(Encoding::UTF_8))
          ruby_bt = Kernel.caller(2)
          java_bt = json['stk'].map { |java_line| "#{java_line[0]}:#{java_line[3]}: in '#{java_line[2]}'" }
          error = RuntimeError.new("#{prefix_msg}: #{json['msg']}")
          error.set_backtrace java_bt + ruby_bt
          raise error
        elsif content_type.start_with? 'text/plain'
          raise "#{prefix_msg}: #{response[:reason]} #{response[:body].force_encoding(Encoding::UTF_8)}"
        else
          raise "#{prefix_msg}: #{response[:reason]}"
        end
      end
    end
  end
end

if RUBY_PLATFORM == "java"
  require_relative 'java_jruby'
else
  require_relative 'java_socket'
end
