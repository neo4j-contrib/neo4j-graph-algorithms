require_relative 'test_helper'

describe Asciidoctor::Diagram::ShaapeBlockMacroProcessor, :broken_on_osx, :broken_on_appveyor do
  it "should generate PNG images when format is set to 'png'" do
    code = <<-eos
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
    eos

    File.write('shaape.txt', code)

    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

shaape::shaape.txt[format="png"]
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
end

describe Asciidoctor::Diagram::ShaapeBlockProcessor, :broken_on_osx, :broken_on_appveyor do
  it "should generate PNG images when format is set to 'png'" do
    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

[shaape, format="png"]
----
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
----
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

  it "should generate SVG images when format is set to 'svg'" do
    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

[shaape, format="svg"]
----
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
----
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.svg/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it "should raise an error when when format is set to an invalid value" do
    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

[shaape, format="foobar"]
----
----
    eos

    expect { load_asciidoc doc }.to raise_error(/support.*format/i)
  end

  it "should not regenerate images when source has not changed" do
    code = <<-eos
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
    eos

    File.write('shaape.txt', code)

    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

shaape::shaape.txt

[shaape, format="png"]
----
    +--------+
    |        |
    | Hello  |
    |   ;)   |
    |        |
    +--------+
----
    eos

    d = load_asciidoc doc
    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil
    target = b.attributes['target']
    mtime1 = File.mtime(target)

    sleep 1

    d = load_asciidoc doc

    mtime2 = File.mtime(target)

    expect(mtime2).to eq mtime1
  end

  it "should handle two block macros with the same source" do
    code = <<-eos
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
    eos

    File.write('shaape.txt', code)

    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

shaape::shaape.txt[]
shaape::shaape.txt[]
    eos

    load_asciidoc doc
    expect(File.exist?('shaape.png')).to be true
  end

  it "should respect target attribute in block macros" do
    code = <<-eos
    +--------+    +-------------+
    |        |     \\           /
    | Hello  |--->  \\ Goodbye /
    |   ;)   |      /         \\
    |        |     /           \\
    +--------+    +-------------+
    eos

    File.write('shaape.txt', code)

    doc = <<-eos
= Hello, Shaape!
Doc Writer <doc@example.com>

== First Section

shaape::shaape.txt["foobar"]
shaape::shaape.txt["foobaz"]
    eos

    load_asciidoc doc
    expect(File.exist?('foobar.png')).to be true
    expect(File.exist?('foobaz.png')).to be true
    expect(File.exist?('shaape.png')).to be false
  end
end