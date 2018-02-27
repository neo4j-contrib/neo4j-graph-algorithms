module Prawn
  class Document
    module Internals
      delegate [:open_graphics_state] => :renderer

      # adds a new, empty content stream to each page. Used in templating so
      # that imported content streams can be left pristine
      #
      def fresh_content_streams(options = {})
        (1..page_count).each do |i|
          go_to_page i
          state.page.new_content_stream
          apply_margin_options(options)
          generate_margin_box
          use_graphic_settings(options[:template])
          forget_text_rendering_mode!
        end
      end
    end
  end
end
