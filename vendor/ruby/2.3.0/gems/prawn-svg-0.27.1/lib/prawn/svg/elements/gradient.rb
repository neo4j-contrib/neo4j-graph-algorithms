class Prawn::SVG::Elements::Gradient < Prawn::SVG::Elements::Base
  TAG_NAME_TO_TYPE = {"linearGradient" => :linear}

  def parse
    # A gradient tag without an ID is inaccessible and can never be used
    raise SkipElementQuietly if attributes['id'].nil?

    assert_compatible_prawn_version
    load_gradient_configuration
    load_coordinates
    load_stops

    document.gradients[attributes['id']] = self

    raise SkipElementQuietly # we don't want anything pushed onto the call stack
  end

  def gradient_arguments(element)
    case @units
    when :bounding_box
      x1, y1, x2, y2 = element.bounding_box
      return if y2.nil?

      width = x2 - x1
      height = y1 - y2

      from = [x1 + width * @x1, y1 - height * @y1]
      to   = [x1 + width * @x2, y1 - height * @y2]

    when :user_space
      from = [@x1, @y1]
      to   = [@x2, @y2]
    end

    {from: from, to: to, stops: @stops}
  end

  private

  def type
    TAG_NAME_TO_TYPE.fetch(name)
  end

  def assert_compatible_prawn_version
    if (Prawn::VERSION.split(".").map(&:to_i) <=> [2, 2, 0]) == -1
      raise SkipElementError, "Prawn 2.2.0+ must be used if you'd like prawn-svg to render gradients"
    end
  end

  def load_gradient_configuration
    @units = attributes["gradientUnits"] == 'userSpaceOnUse' ? :user_space : :bounding_box

    if transform = attributes["gradientTransform"]
      matrix = transform.split(COMMA_WSP_REGEXP).map(&:to_f)
      if matrix != [1, 0, 0, 1, 0, 0]
        raise SkipElementError, "prawn-svg does not yet support gradients with a non-identity gradientTransform attribute"
      end
    end

    if (spread_method = attributes['spreadMethod']) && spread_method != "pad"
      warnings << "prawn-svg only currently supports the 'pad' spreadMethod attribute value"
    end
  end

  def load_coordinates
    case @units
    when :bounding_box
      @x1 = parse_zero_to_one(attributes["x1"], 0)
      @y1 = parse_zero_to_one(attributes["y1"], 0)
      @x2 = parse_zero_to_one(attributes["x2"], 1)
      @y2 = parse_zero_to_one(attributes["y2"], 0)

    when :user_space
      @x1 = x(attributes["x1"])
      @y1 = y(attributes["y1"])
      @x2 = x(attributes["x2"])
      @y2 = y(attributes["y2"])
    end
  end

  def load_stops
    stop_elements = source.elements.map do |child|
      element = Prawn::SVG::Elements::Base.new(document, child, [], Prawn::SVG::State.new)
      element.process
      element
    end.select do |element|
      element.name == 'stop' && element.attributes["offset"]
    end

    @stops = stop_elements.each.with_object([]) do |child, result|
      offset = parse_zero_to_one(child.attributes["offset"])

      # Offsets must be strictly increasing (SVG 13.2.4)
      if result.last && result.last.first > offset
        offset = result.last.first
      end

      if color_hex = Prawn::SVG::Color.color_to_hex(child.properties.stop_color)
        result << [offset, color_hex]
      end
    end

    raise SkipElementError, "gradient does not have any valid stops" if @stops.empty?

    @stops.unshift([0, @stops.first.last]) if @stops.first.first > 0
    @stops.push([1, @stops.last.last])     if @stops.last.first  < 1
  end

  def parse_zero_to_one(string, default = 0)
    string = string.to_s.strip
    return default if string == ""

    value = string.to_f
    value /= 100.0 if string[-1..-1] == '%'
    [0.0, value, 1.0].sort[1]
  end
end
