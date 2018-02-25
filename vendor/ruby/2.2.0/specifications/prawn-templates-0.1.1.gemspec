# -*- encoding: utf-8 -*-
# stub: prawn-templates 0.1.1 ruby lib

Gem::Specification.new do |s|
  s.name = "prawn-templates"
  s.version = "0.1.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.3.6") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Gregory Brown", "Brad Ediger", "Daniel Nelson", "Jonathan Greenberg", "James Healy", "Burkhard Vogel-Kreykenbohm"]
  s.date = "2017-03-24"
  s.description = "A extention to prawn that allows to include other pdfs either as background to write upon or to combine several pdf documents into one."
  s.email = ["gregory.t.brown@gmail.com", "brad@bradediger.com", "dnelson@bluejade.com", "greenberg@entryway.net", "jimmy@deefa.com", "b.vogel@buddyandselly.com"]
  s.homepage = "https://github.com/prawnpdf/prawn-templates"
  s.licenses = ["Nonstandard", "GPL-2.0", "GPL-3.0"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3")
  s.rubygems_version = "2.4.5.1"
  s.summary = "Prawn::Templates allows using PDFs as templates in Prawn"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<pdf-reader>, ["~> 2.0"])
      s.add_runtime_dependency(%q<prawn>, ["~> 2.2"])
      s.add_development_dependency(%q<pdf-inspector>, ["~> 1.3"])
      s.add_development_dependency(%q<rspec>, ["~> 3.0"])
      s.add_development_dependency(%q<rake>, ["~> 12.0"])
      s.add_development_dependency(%q<rubocop>, ["~> 0.47"])
      s.add_development_dependency(%q<rubocop-rspec>, ["~> 1.10"])
    else
      s.add_dependency(%q<pdf-reader>, ["~> 2.0"])
      s.add_dependency(%q<prawn>, ["~> 2.2"])
      s.add_dependency(%q<pdf-inspector>, ["~> 1.3"])
      s.add_dependency(%q<rspec>, ["~> 3.0"])
      s.add_dependency(%q<rake>, ["~> 12.0"])
      s.add_dependency(%q<rubocop>, ["~> 0.47"])
      s.add_dependency(%q<rubocop-rspec>, ["~> 1.10"])
    end
  else
    s.add_dependency(%q<pdf-reader>, ["~> 2.0"])
    s.add_dependency(%q<prawn>, ["~> 2.2"])
    s.add_dependency(%q<pdf-inspector>, ["~> 1.3"])
    s.add_dependency(%q<rspec>, ["~> 3.0"])
    s.add_dependency(%q<rake>, ["~> 12.0"])
    s.add_dependency(%q<rubocop>, ["~> 0.47"])
    s.add_dependency(%q<rubocop-rspec>, ["~> 1.10"])
  end
end
