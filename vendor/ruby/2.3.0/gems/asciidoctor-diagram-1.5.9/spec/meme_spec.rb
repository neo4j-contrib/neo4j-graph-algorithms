require_relative 'test_helper'

describe Asciidoctor::Diagram::MemeBlockMacroProcessor do
  it "should generate PNG images when format is set to 'png'" do
    FileUtils.cp(
        File.expand_path('man.jpg', File.dirname(__FILE__)),
        File.expand_path('man.jpg', Dir.getwd)
    )

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

meme::man.jpg[I don't always // write unit tests, but when I do // they generate memes, format=png, options=noupcase]
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.png$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it "should generate GIF images when format is set to 'gif'" do
    FileUtils.cp(
        File.expand_path('man.jpg', File.dirname(__FILE__)),
        File.expand_path('man.jpg', Dir.getwd)
    )

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

meme::man.jpg[I don't always // write unit tests, but when I do // they generate memes, format=gif, options=noupcase]
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.gif/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end
end
