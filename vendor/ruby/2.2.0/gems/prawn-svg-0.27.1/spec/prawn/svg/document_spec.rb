require File.dirname(__FILE__) + '/../../spec_helper'

describe Prawn::SVG::Document do
  let(:bounds) { [100, 100] }
  let(:options) { {} }

  describe "#initialize" do
    context "when unparsable XML is provided" do
      let(:svg) { "this isn't SVG data" }

      it "raises an exception" do
        expect {
          Prawn::SVG::Document.new(svg, bounds, options)
        }.to raise_error Prawn::SVG::Document::InvalidSVGData, "The data supplied is not a valid SVG document."
      end
    end

    context "when the user passes in a filename instead of SVG data" do
      let(:svg) { "some_file.svg" }

      it "raises an exception letting them know what they've done" do
        expect {
          Prawn::SVG::Document.new(svg, bounds, options)
        }.to raise_error Prawn::SVG::Document::InvalidSVGData, "The data supplied is not a valid SVG document.  It looks like you've supplied a filename instead; use IO.read(filename) to get the data before you pass it to prawn-svg."
      end
    end
  end

  describe "#parse_style_elements" do
    let(:svg) do
      <<-SVG
<svg>
  <some-tag>
    <style>a
  before&gt;
  x <![CDATA[ y
  inside <>&gt;
  k ]]> j
  after
z</style>
  </some-tag>

  <other-tag>
    <more-tag>
      <style>hello</style>
    </more-tag>
  </other-tag>
</svg>
      SVG
    end

    it "scans the document for style tags and adds the style information to the css parser" do
      css_parser = instance_double(CssParser::Parser)

      expect(css_parser).to receive(:add_block!).with("a\n  before>\n  x  y\n  inside <>&gt;\n  k  j\n  after\nz")
      expect(css_parser).to receive(:add_block!).with("hello")

      Prawn::SVG::Document.new(svg, bounds, options, css_parser: css_parser)
    end
  end
end
