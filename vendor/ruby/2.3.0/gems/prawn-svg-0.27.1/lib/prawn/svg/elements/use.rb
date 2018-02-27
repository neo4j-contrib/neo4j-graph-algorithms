class Prawn::SVG::Elements::Use < Prawn::SVG::Elements::Base
  attr_reader :referenced_element

  def parse
    require_attributes 'xlink:href'

    href = attributes['xlink:href']

    if href[0..0] != '#'
      raise SkipElementError, "use tag has an href that is not a reference to an id; this is not supported"
    end

    id = href[1..-1]
    @referenced_element = @document.elements_by_id[id]

    if referenced_element.nil?
      raise SkipElementError, "no tag with ID '#{id}' was found, referenced by use tag"
    end

    state.inside_use = true

    @x = attributes['x']
    @y = attributes['y']
  end

  def container?
    true
  end

  def apply
    if @x || @y
      add_call_and_enter "translate", x_pixels(@x || 0), -y_pixels(@y || 0)
    end
  end

  def process_child_elements
    add_call "save"

    child = referenced_element.class.new(referenced_element.document, referenced_element.source, calls, state.dup)
    child.process

    add_call "restore"
  end
end
