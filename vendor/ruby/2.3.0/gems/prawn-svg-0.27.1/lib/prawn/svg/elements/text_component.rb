class Prawn::SVG::Elements::TextComponent < Prawn::SVG::Elements::DepthFirstBase
  attr_reader :commands

  Printable = Struct.new(:element, :text, :leading_space?, :trailing_space?)
  PositionsList = Struct.new(:x, :y, :dx, :dy, :rotation, :parent)

  def parse
    state.text.x = attributes['x'].split(COMMA_WSP_REGEXP).collect {|n| x(n)} if attributes['x']
    state.text.y = attributes['y'].split(COMMA_WSP_REGEXP).collect {|n| y(n)} if attributes['y']
    state.text.dx = attributes['dx'].split(COMMA_WSP_REGEXP).collect {|n| x_pixels(n)} if attributes['dx']
    state.text.dy = attributes['dy'].split(COMMA_WSP_REGEXP).collect {|n| y_pixels(n)} if attributes['dy']
    state.text.rotation = attributes['rotate'].split(COMMA_WSP_REGEXP).collect(&:to_f) if attributes['rotate']

    @commands = []

    svg_text_children.each do |child|
      if child.node_type == :text
        append_text(child)
      else
        case child.name
        when 'tspan', 'tref'
          append_child(child)
        else
          warnings << "Unknown tag '#{child.name}' inside text tag; ignoring"
        end
      end
    end
  end

  def apply
    raise SkipElementQuietly if computed_properties.display == "none"

    font = select_font
    apply_font(font) if font

    # text_anchor isn't a Prawn option; we have to do some math to support it
    # and so we handle this in Prawn::SVG::Interface#rewrite_call_arguments
    opts = {
      size:        computed_properties.numerical_font_size,
      style:       font && font.subfamily,
      text_anchor: computed_properties.text_anchor
    }

    spacing = computed_properties.letter_spacing
    spacing = spacing == 'normal' ? 0 : pixels(spacing)

    add_call_and_enter 'character_spacing', spacing

    @commands.each do |command|
      case command
      when Printable
        apply_text(command.text, opts)
      when self.class
        add_call 'save'
        command.apply_step(calls)
        add_call 'restore'
      else
        raise
      end
    end

    # It's possible there was no text to render.  In that case, add a 'noop' so
    # character_spacing doesn't blow up when it finds it doesn't have a block to execute.
    add_call 'noop' if calls.empty?
  end

  protected

  def append_text(child)
    if state.preserve_space
      text = child.value.tr("\n\t", ' ')
    else
      text = child.value.tr("\n", '').tr("\t", ' ')
      leading = text[0] == ' '
      trailing = text[-1] == ' '
      text = text.strip.gsub(/ {2,}/, ' ')
    end

    @commands << Printable.new(self, text, leading, trailing)
  end

  def append_child(child)
    new_state = state.dup
    new_state.text = PositionsList.new([], [], [], [], [], state.text)

    element = self.class.new(document, child, calls, new_state)
    @commands << element
    element.parse_step
  end

  def apply_text(text, opts)
    while text != ""
      x = y = dx = dy = rotate = nil
      remaining = rotation_remaining = false

      list = state.text
      while list
        shifted = list.x.shift
        x ||= shifted
        shifted = list.y.shift
        y ||= shifted
        shifted = list.dx.shift
        dx ||= shifted
        shifted = list.dy.shift
        dy ||= shifted

        shifted = list.rotation.length > 1 ? list.rotation.shift : list.rotation.first
        if shifted && rotate.nil?
          rotate = shifted
          remaining ||= list.rotation != [0]
        end

        remaining ||= list.x.any? || list.y.any? || list.dx.any? || list.dy.any? || (rotate && rotate != 0)
        rotation_remaining ||= list.rotation.length > 1
        list = list.parent
      end

      opts[:at] = [x || :relative, y || :relative]
      opts[:offset] = [dx || 0, dy || 0]

      if rotate && rotate != 0
        opts[:rotate] = -rotate
      else
        opts.delete(:rotate)
      end

      if remaining
        add_call 'draw_text', text[0..0], opts.dup
        text = text[1..-1]
      else
        add_call 'draw_text', text, opts.dup

        # we can get to this path with rotations still pending
        # solve this by shifting them out by the number of
        # characters we've just drawn
        shift = text.length - 1
        if rotation_remaining && shift > 0
          list = state.text
          while list
            count = [shift, list.rotation.length - 1].min
            list.rotation.shift(count) if count > 0
            list = list.parent
          end
        end

        break
      end
    end
  end

  def svg_text_children
    text_children.select do |child|
      child.node_type == :text || child.namespace == SVG_NAMESPACE || child.namespace == ''
    end
  end

  def text_children
    if name == 'tref'
      reference = find_referenced_element
      reference ? reference.source.children : []
    else
      source.children
    end
  end

  def find_referenced_element
    href = attributes['xlink:href']

    if href && href[0..0] == '#'
      element = document.elements_by_id[href[1..-1]]
      element if element.name == 'text'
    end
  end

  def select_font
    font_families = [computed_properties.font_family, document.fallback_font_name]
    font_style = :italic if computed_properties.font_style == 'italic'
    font_weight = Prawn::SVG::Font.weight_for_css_font_weight(computed_properties.font_weight)

    font_families.compact.each do |name|
      font = document.font_registry.load(name, font_weight, font_style)
      return font if font
    end

    warnings << "Font family '#{computed_properties.font_family}' style '#{computed_properties.font_style}' is not a known font, and the fallback font could not be found."
    nil
  end

  def apply_font(font)
    add_call 'font', font.name, style: font.subfamily
  end
end
