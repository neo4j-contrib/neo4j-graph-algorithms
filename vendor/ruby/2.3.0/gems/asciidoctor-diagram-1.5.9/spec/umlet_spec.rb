require_relative 'test_helper'

code = <<-eos
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<diagram program="umlet" version="14.2">
  <zoom_level>10</zoom_level>
  <element>
    <id>UMLActor</id>
    <coordinates>
      <x>20</x>
      <y>20</y>
      <w>60</w>
      <h>120</h>
    </coordinates>
    <panel_attributes>Hello
AsciiDoc</panel_attributes>
    <additional_attributes/>
  </element>
</diagram>
eos


describe Asciidoctor::Diagram::UmletBlockMacroProcessor do
  it "should generate SVG images when format omitted" do
    File.write('umlet.uxf', code)

    doc = <<-eos
= Hello, Umlet!
Doc Writer <doc@example.com>

== First Section

umlet::umlet.uxf[]
    eos
    
    d = load_asciidoc doc
    expect(d).to_not be_nil
  
    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.svg$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end
end


describe Asciidoctor::Diagram::UmletBlockProcessor do
  it "should generate SVG images when format is omitted" do
    doc = <<-eos
= Hello, Umlet!
Doc Writer <doc@example.com>

== First Section

[umlet]
----
#{code}
----
    eos
    
    d = load_asciidoc doc
    expect(d).to_not be_nil
  
    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.svg$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end
end
