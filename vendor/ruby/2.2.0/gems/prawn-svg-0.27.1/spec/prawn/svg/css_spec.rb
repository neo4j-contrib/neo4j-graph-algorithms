require 'spec_helper'

RSpec.describe Prawn::SVG::CSS do
  describe "#parse_font_family_string" do
    it "correctly handles quotes and escaping" do
      tests = {
        "" => [],
        "font" => ["font"],
        "font name, other font" => ["font name", "other font"],
        "'font name', other font" => ["font name", "other font"],
        "'font, name', other font" => ["font, name", "other font"],
        '"font name", other font' => ["font name", "other font"],
        '"font, name", other font' => ["font, name", "other font"],
        'weird \\" name' => ['weird " name'],
        'weird\\, name' => ["weird, name"],
        ' stupid , spacing ' => ["stupid", "spacing"],
      }

      tests.each do |string, expected|
        expect(Prawn::SVG::CSS.parse_font_family_string(string)).to eq expected
      end
    end
  end
end
