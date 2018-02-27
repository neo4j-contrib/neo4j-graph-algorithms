module Prawn::SVG::Attributes::Transform
  def parse_transform_attribute_and_call
    return unless transform = attributes['transform']

    parse_css_method_calls(transform).each do |name, arguments|
      case name
      when 'translate'
        x, y = arguments
        add_call_and_enter name, x_pixels(x.to_f), -y_pixels(y.to_f)

      when 'rotate'
        r, x, y = arguments.collect {|a| a.to_f}
        case arguments.length
        when 1
          add_call_and_enter name, -r, :origin => [0, y('0')]
        when 3
          add_call_and_enter name, -r, :origin => [x(x), y(y)]
        else
          warnings << "transform 'rotate' must have either one or three arguments"
        end

      when 'scale'
        x_scale = arguments[0].to_f
        y_scale = (arguments[1] || x_scale).to_f
        add_call_and_enter "transformation_matrix", x_scale, 0, 0, y_scale, 0, 0

      when 'matrix'
        if arguments.length != 6
          warnings << "transform 'matrix' must have six arguments"
        else
          a, b, c, d, e, f = arguments.collect {|argument| argument.to_f}
          add_call_and_enter "transformation_matrix", a, -b, -c, d, x_pixels(e), -y_pixels(f)
        end

      else
        warnings << "Unknown transformation '#{name}'; ignoring"
      end
    end
  end

  private

  def parse_css_method_calls(string)
    string.scan(/\s*(\w+)\(([^)]+)\)\s*/).collect do |call|
      name, argument_string = call
      arguments = argument_string.strip.split(/\s*[,\s]\s*/)
      [name, arguments]
    end
  end
end
