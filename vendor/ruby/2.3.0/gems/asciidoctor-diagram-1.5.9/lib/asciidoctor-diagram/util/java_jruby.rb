require 'java'
require 'stringio'

module Asciidoctor
  module Diagram
    # @private
    module Java
      def self.load
        if @loaded
          return
        end

        classpath.flatten.each { |j| require j }
        @loaded = true
      end

      def self.send_request(req)
        cp = ::Java.org.asciidoctor.diagram.CommandProcessor.new()

        req_io = StringIO.new
        format_request(req, req_io)
        req_io.close

        response = cp.processRequest(req_io.string.to_java_bytes)

        resp_io = StringIO.new(String.from_java_bytes(response))
        resp = parse_response(resp_io)
        resp_io.close

        resp
      end
    end
  end
end