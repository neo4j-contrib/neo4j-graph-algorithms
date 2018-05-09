require_relative 'test_helper'

describe Asciidoctor::Diagram::PlantUmlBlockMacroProcessor do
  it "should generate PNG images when format is set to 'png'" do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt[format="png"]
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

  it "should generate PNG images when format is set to 'png'" do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt[format="png"]
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

  it 'should support substitutions in diagram code' do
    code = <<-eos
class {parent-class}
class {child-class}
{parent-class} <|-- {child-class}
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>
:parent-class: ParentClass
:child-class: ChildClass

== First Section

plantuml::plantuml.txt[format="svg", subs=attributes+]
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    content = File.read(target, :encoding => Encoding::UTF_8)
    expect(content).to include('ParentClass')
    expect(content).to include('ChildClass')
  end

  it 'should support substitutions in the target attribute' do
    code = <<-eos
class {parent-class}
class {child-class}
{parent-class} <|-- {child-class}
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>
:file: plantuml
:parent-class: ParentClass
:child-class: ChildClass

== First Section

plantuml::{file}.txt[format="svg", subs=attributes+]
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    content = File.read(target, :encoding => Encoding::UTF_8)
    expect(content).to include('ParentClass')
    expect(content).to include('ChildClass')
  end

  it 'should support substitutions in the format attribute' do
    code = <<-eos
class Parent
class Child
Parent <|-- Child
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>
:file: plantuml
:plantumlformat: png

== First Section

plantuml::{file}.txt[format="{plantumlformat}", subs=attributes+]
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(target).to match(/\.png$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it 'should resolve !include directives with relative paths' do
    included = <<-eos
interface List
List : int size()
List : void clear()
    eos

    code = <<-eos
!include list.iuml
List <|.. ArrayList
    eos

    Dir.mkdir('dir')
    File.write('dir/list.iuml', included)
    File.write('dir/plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>
:parent-class: ParentClass
:child-class: ChildClass

== First Section

plantuml::dir/plantuml.txt[format="svg", subs=attributes+]
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}

    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    content = File.read(target, :encoding => Encoding::UTF_8)
    expect(content).to_not include('!include')
  end

  it 'should generate blocks with figure captions' do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

.This is a UML diagram
plantuml::plantuml.txt[format="png"]
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.caption).to match(/Figure \d+/)
  end
end

describe Asciidoctor::Diagram::PlantUmlBlockProcessor do
  it "should generate PNG images when format is set to 'png'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
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
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="svg"]
----
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
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

  it "should generate literal blocks when format is set to 'txt'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="txt"]
----
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
----
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :literal }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :verbatim

    expect(b.attributes['target']).to be_nil
  end

  it 'should raise an error when when format is set to an invalid value' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="foobar"]
----
----
    eos

    expect { load_asciidoc doc }.to raise_error(/support.*format/i)
  end

  it 'should use plantuml configuration when specified as a document attribute' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>
:plantuml-config: test.config
:plantuml-format: svg

== First Section

[plantuml]
----
actor Foo1
boundary Foo2
Foo1 -> Foo2 : To boundary
----
    eos

    config = <<-eos
skinparam ArrowColor #DEADBE
    eos

    File.open('test.config', 'w') do |f|
      f.write config
    end

    d = load_asciidoc doc
    b = d.find { |bl| bl.context == :image }

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(File.exist?(target)).to be true

    svg = File.read(target, :encoding => Encoding::UTF_8)
    expect(svg).to match(/<path.*fill="#DEADBE"/)
  end

  it 'should not regenerate images when source has not changed' do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt

[plantuml, format="png"]
----
actor Foo1
boundary Foo2
Foo1 -> Foo2 : To boundary
----
    eos

    d = load_asciidoc doc
    b = d.find { |bl| bl.context == :image }
    target = b.attributes['target']
    mtime1 = File.mtime(target)

    sleep 1

    d = load_asciidoc doc

    mtime2 = File.mtime(target)

    expect(mtime2).to eq mtime1
  end

  it 'should handle two block macros with the same source' do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt[]
plantuml::plantuml.txt[]
    eos

    load_asciidoc doc
    expect(File.exist?('plantuml.png')).to be true
  end

  it 'should respect target attribute in block macros' do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt["foobar"]
plantuml::plantuml.txt["foobaz"]
    eos

    load_asciidoc doc
    expect(File.exist?('foobar.png')).to be true
    expect(File.exist?('foobaz.png')).to be true
    expect(File.exist?('plantuml.png')).to be false
  end

  it 'should respect target attribute values with relative paths in block macros' do
    code = <<-eos
User -> (Start)
User --> (Use the application) : Label

:Main Admin: ---> (Use the application) : Another label
    eos

    File.write('plantuml.txt', code)

    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

plantuml::plantuml.txt["test/foobar"]
plantuml::plantuml.txt["test2/foobaz"]
    eos

    load_asciidoc doc
    expect(File.exist?('test/foobar.png')).to be true
    expect(File.exist?('test2/foobaz.png')).to be true
    expect(File.exist?('plantuml.png')).to be false
  end

  it 'should write files to outdir if set' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="svg"]
----
actor Foo1
boundary Foo2
Foo1 -> Foo2 : To boundary
----
    eos

    d = load_asciidoc doc, {:attributes => {'outdir' => 'foo'}}
    b = d.find { |bl| bl.context == :image }

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(File.exist?(target)).to be false
    expect(File.exist?(File.expand_path(target, 'foo'))).to be true
  end

  it 'should write files to imagesoutdir if set' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="svg"]
----
actor Foo1
boundary Foo2
Foo1 -> Foo2 : To boundary
----
    eos

    d = load_asciidoc doc, {:attributes => {'imagesoutdir' => 'bar', 'outdir' => 'foo'}}
    b = d.find { |bl| bl.context == :image }

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(File.exist?(target)).to be false
    expect(File.exist?(File.expand_path(target, 'bar'))).to be true
    expect(File.exist?(File.expand_path(target, 'foo'))).to be false
  end

  it 'should omit width/height attributes when generating docbook' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
User -> (Start)
----
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'docbook5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to be_nil
    expect(b.attributes['height']).to be_nil
  end

  it 'should generate blocks with figure captions' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

.Caption for my UML diagram
[plantuml, format="png"]
----
User -> (Start)
----
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.caption).to match(/Figure \d+/)
  end

  it 'should support salt diagrams using salt block type' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[salt, format="png"]
----
{
  Just plain text
  [This is my button]
  ()  Unchecked radio
  (X) Checked radio
  []  Unchecked box
  [X] Checked box
  "Enter text here   "
  ^This is a droplist^
}
----
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'docbook5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to be_nil
    expect(b.attributes['height']).to be_nil
  end

  it 'should support salt diagrams using plantuml block type' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
salt
{
  Just plain text
  [This is my button]
  ()  Unchecked radio
  (X) Checked radio
  []  Unchecked box
  [X] Checked box
  "Enter text here   "
  ^This is a droplist^
}
----
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'docbook5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to be_nil
    expect(b.attributes['height']).to be_nil
  end

  it 'should support salt diagrams containing tree widgets' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
salt
{
{T
+A
++a
}
}
----
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'docbook5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to be_nil
    expect(b.attributes['height']).to be_nil
  end

  it 'should support scaling diagrams' do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
A -> B
----
    eos

    scaled_doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png", scale="1.5"]
----
A -> B
----
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}
    unscaled_image = d.find { |bl| bl.context == :image }

    d = load_asciidoc scaled_doc, :attributes => {'backend' => 'html5'}
    scaled_image = d.find { |bl| bl.context == :image }

    expect(scaled_image.attributes['width']).to be_within(1).of(unscaled_image.attributes['width'] * 1.5)
    expect(scaled_image.attributes['height']).to be_within(1).of(unscaled_image.attributes['height'] * 1.5)
  end

  it 'should handle embedded creole images correctly' do
    creole_doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="png"]
----
:* You can change <color:red>text color</color>
* You can change <back:cadetblue>background color</back>
* You can change <size:18>size</size>
* You use <u>legacy</u> <b>HTML <i>tag</i></b>
* You use <u:red>color</u> <s:green>in HTML</s> <w:#0000FF>tag</w>
* Use image : <img:sourceforge.jpg>
* Use image : <img:http://www.foo.bar/sourceforge.jpg>
* Use image : <img:file:///sourceforge.jpg>

;
----
    eos

    load_asciidoc creole_doc, :attributes => {'backend' => 'html5'}

    # No real way to assert this since PlantUML doesn't produce an error on file not found
  end

  it 'should resolve !include directives with relative paths' do
    included = <<-eos
interface List
List : int size()
List : void clear()
    eos

    Dir.mkdir('dir')
    File.write('dir/List.iuml', included)

    creole_doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml, format="svg"]
----
!include dir/List.iuml
List <|.. ArrayList
----
    eos

    d = load_asciidoc creole_doc, :attributes => {'backend' => 'html5'}

    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    content = File.read(target, :encoding => Encoding::UTF_8)
    expect(content).to_not include('!include')
  end

  it 'should support substitutions' do
    doc = <<-eos
= Hello, PlantUML!
:parent-class: ParentClass
:child-class: ChildClass

[plantuml,class-inheritence,svg,subs=attributes+]
....
class {parent-class}
class {child-class}
{parent-class} <|-- {child-class}
....
    eos

    d = load_asciidoc doc, :attributes => {'backend' => 'html5'}
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    target = b.attributes['target']
    expect(File.exist?(target)).to be true

    content = File.read(target, :encoding => Encoding::UTF_8)
    expect(content).to include('ParentClass')
    expect(content).to include('ChildClass')
  end

  it "should generate PNG images for jlatexmath blocks when format is set to 'png'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml,format="png"]
----
@startlatex
e^{i\\pi} + 1 = 0
@endlatex
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

  it "should generate SVG images for jlatexmath blocks when format is set to 'svg'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml,format="svg"]
----
@startlatex
e^{i\\pi} + 1 = 0
@endlatex
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
  end

  it "should generate PNG images for diagrams with latex tags when format is set to 'png'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml,format="png"]
----
:<latex>P(y|\\mathbf{x}) \\mbox{ or } f(\\mathbf{x})+\\epsilon</latex>;
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

  it "should generate SVG images for diagrams with latex tags when format is set to 'svg'" do
    doc = <<-eos
= Hello, PlantUML!
Doc Writer <doc@example.com>

== First Section

[plantuml,format="svg"]
----
:<latex>P(y|\\mathbf{x}) \\mbox{ or } f(\\mathbf{x})+\\epsilon</latex>;
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
