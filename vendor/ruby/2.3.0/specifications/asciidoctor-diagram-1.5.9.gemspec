# -*- encoding: utf-8 -*-
# stub: asciidoctor-diagram 1.5.9 ruby lib

Gem::Specification.new do |s|
  s.name = "asciidoctor-diagram".freeze
  s.version = "1.5.9"

  s.required_rubygems_version = Gem::Requirement.new(">= 0".freeze) if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib".freeze]
  s.authors = ["Pepijn Van Eeckhoudt".freeze]
  s.date = "2018-03-28"
  s.description = "Asciidoctor diagramming extension".freeze
  s.email = ["pepijn@vaneeckhoudt.net".freeze]
  s.homepage = "https://github.com/asciidoctor/asciidoctor-diagram".freeze
  s.licenses = ["MIT".freeze]
  s.rubygems_version = "2.5.2.1".freeze
  s.summary = "An extension for asciidoctor that adds support for UML diagram generation using PlantUML".freeze

  s.installed_by_version = "2.5.2.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<bundler>.freeze, ["~> 1.3"])
      s.add_development_dependency(%q<rake>.freeze, [">= 0"])
      s.add_development_dependency(%q<rspec>.freeze, [">= 0"])
      s.add_runtime_dependency(%q<asciidoctor>.freeze, ["~> 1.5.0"])
    else
      s.add_dependency(%q<bundler>.freeze, ["~> 1.3"])
      s.add_dependency(%q<rake>.freeze, [">= 0"])
      s.add_dependency(%q<rspec>.freeze, [">= 0"])
      s.add_dependency(%q<asciidoctor>.freeze, ["~> 1.5.0"])
    end
  else
    s.add_dependency(%q<bundler>.freeze, ["~> 1.3"])
    s.add_dependency(%q<rake>.freeze, [">= 0"])
    s.add_dependency(%q<rspec>.freeze, [">= 0"])
    s.add_dependency(%q<asciidoctor>.freeze, ["~> 1.5.0"])
  end
end
