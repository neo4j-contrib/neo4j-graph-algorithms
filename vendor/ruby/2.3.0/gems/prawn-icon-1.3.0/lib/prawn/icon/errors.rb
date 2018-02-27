module Prawn
  class Icon
    module Errors
      # Error raised when an icon glyph is not found
      #
      IconNotFound = Class.new(StandardError)

      # Error raised when an icon key is not provided
      #
      IconKeyEmpty = Class.new(StandardError)
    end
  end
end
