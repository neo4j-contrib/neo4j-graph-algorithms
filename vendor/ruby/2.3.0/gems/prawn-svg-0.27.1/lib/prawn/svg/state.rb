class Prawn::SVG::State
  attr_accessor :disable_drawing,
    :text, :preserve_space,
    :fill_opacity, :stroke_opacity, :stroke_width,
    :computed_properties,
    :viewport_sizing,
    :inside_use

  def initialize
    @stroke_width = 1
    @fill_opacity = 1
    @stroke_opacity = 1
    @computed_properties = Prawn::SVG::Properties.new.load_default_stylesheet
  end

  def initialize_dup(other)
    @computed_properties = @computed_properties.dup
  end
end
