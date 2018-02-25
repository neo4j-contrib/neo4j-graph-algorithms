# -*- encoding: utf-8 -*-
# stub: prawn-table 0.2.2 ruby lib

Gem::Specification.new do |s|
  s.name = "prawn-table"
  s.version = "0.2.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.3.6") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Gregory Brown", "Brad Ediger", "Daniel Nelson", "Jonathan Greenberg", "James Healy", "Hartwig Brandl"]
  s.date = "2015-07-16"
  s.description = "  Prawn::Table provides tables for the Prawn PDF toolkit\n"
  s.email = ["gregory.t.brown@gmail.com", "brad@bradediger.com", "dnelson@bluejade.com", "greenberg@entryway.net", "jimmy@deefa.com", "mail@hartwigbrandl.at"]
  s.homepage = "https://github.com/prawnpdf/prawn-table"
  s.licenses = ["RUBY", "GPL-2", "GPL-3"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3")
  s.rubyforge_project = "prawn"
  s.rubygems_version = "2.4.5.1"
  s.summary = "Provides tables for PrawnPDF"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<prawn>, ["< 3.0.0", ">= 1.3.0"])
      s.add_development_dependency(%q<pdf-inspector>, ["~> 1.1.0"])
      s.add_development_dependency(%q<yard>, [">= 0"])
      s.add_development_dependency(%q<rspec>, ["= 2.14.1"])
      s.add_development_dependency(%q<mocha>, [">= 0"])
      s.add_development_dependency(%q<rake>, [">= 0"])
      s.add_development_dependency(%q<simplecov>, [">= 0"])
      s.add_development_dependency(%q<prawn-manual_builder>, [">= 0.2.0"])
      s.add_development_dependency(%q<pdf-reader>, ["~> 1.2"])
    else
      s.add_dependency(%q<prawn>, ["< 3.0.0", ">= 1.3.0"])
      s.add_dependency(%q<pdf-inspector>, ["~> 1.1.0"])
      s.add_dependency(%q<yard>, [">= 0"])
      s.add_dependency(%q<rspec>, ["= 2.14.1"])
      s.add_dependency(%q<mocha>, [">= 0"])
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<simplecov>, [">= 0"])
      s.add_dependency(%q<prawn-manual_builder>, [">= 0.2.0"])
      s.add_dependency(%q<pdf-reader>, ["~> 1.2"])
    end
  else
    s.add_dependency(%q<prawn>, ["< 3.0.0", ">= 1.3.0"])
    s.add_dependency(%q<pdf-inspector>, ["~> 1.1.0"])
    s.add_dependency(%q<yard>, [">= 0"])
    s.add_dependency(%q<rspec>, ["= 2.14.1"])
    s.add_dependency(%q<mocha>, [">= 0"])
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<simplecov>, [">= 0"])
    s.add_dependency(%q<prawn-manual_builder>, [">= 0.2.0"])
    s.add_dependency(%q<pdf-reader>, ["~> 1.2"])
  end
end
