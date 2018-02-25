require 'spec_helper'

describe Prawn::SVG::Attributes::Transform do
  class TransformTestElement
    include Prawn::SVG::Attributes::Transform

    attr_accessor :attributes, :warnings

    def initialize
      @warnings = []
      @attributes = {}
    end
  end

  let(:element) { TransformTestElement.new }

  describe "#parse_transform_attribute_and_call" do
    subject { element.send :parse_transform_attribute_and_call }

    describe "translate" do
      it "handles a missing y argument" do
        expect(element).to receive(:add_call_and_enter).with('translate', -5.5, 0)
        expect(element).to receive(:x_pixels).with(-5.5).and_return(-5.5)
        expect(element).to receive(:y_pixels).with(0.0).and_return(0.0)

        element.attributes['transform'] = 'translate(-5.5)'
        subject
      end
    end

    describe "rotate" do
      it "handles a single angle argument" do
        expect(element).to receive(:add_call_and_enter).with('rotate', -5.5, :origin => [0, 0])
        expect(element).to receive(:y).with('0').and_return(0)

        element.attributes['transform'] = 'rotate(5.5)'
        subject
      end

      it "handles three arguments" do
        expect(element).to receive(:add_call_and_enter).with('rotate', -5.5, :origin => [1.0, 2.0])
        expect(element).to receive(:x).with(1.0).and_return(1.0)
        expect(element).to receive(:y).with(2.0).and_return(2.0)

        element.attributes['transform'] = 'rotate(5.5 1 2)'
        subject
      end

      it "does nothing and warns if two arguments" do
        expect(element).to receive(:warnings).and_return([])
        element.attributes['transform'] = 'rotate(5.5 1)'
        subject
      end
    end
  end
end
