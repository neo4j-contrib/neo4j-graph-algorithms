require 'tempfile'
require 'open3'

module Asciidoctor
  module Diagram
    # @private
    module Cli
      def self.run(*args)
        stdout, stderr, status = Open3.capture3(*args)

        if status.exitstatus != 0
          raise "#{File.basename(args[0])} failed: #{stdout.empty? ? stderr : stdout}"
        end

        {
            :out => stdout,
            :err => stderr,
            :status => status
        }
      end
    end
  end
end
