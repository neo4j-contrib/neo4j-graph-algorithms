# -*- encoding: utf-8 -*-
# stub: prawn-icon 1.3.0 ruby lib

Gem::Specification.new do |s|
  s.name = "prawn-icon"
  s.version = "1.3.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.3.6") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Jesse Doyle"]
  s.date = "2016-10-15"
  s.description = "  Prawn::Icon provides various icon fonts including\n  FontAwesome, Foundation Icons and GitHub Octicons\n  for use with the Prawn PDF toolkit.\n"
  s.email = ["jdoyle@ualberta.ca"]
  s.homepage = "https://github.com/jessedoyle/prawn-icon/"
  s.licenses = ["RUBY", "GPL-2", "GPL-3"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3")
  s.rubygems_version = "2.4.5.1"
  s.summary = "Provides icon fonts for PrawnPDF"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<prawn>, ["< 3.0.0", ">= 1.1.0"])
      s.add_development_dependency(%q<pdf-inspector>, ["~> 1.2.1"])
      s.add_development_dependency(%q<rspec>, ["~> 3.5.0"])
      s.add_development_dependency(%q<rubocop>, ["~> 0.44.1"])
      s.add_development_dependency(%q<rake>, [">= 0"])
      s.add_development_dependency(%q<pdf-reader>, ["~> 1.4"])
      s.add_development_dependency(%q<simplecov>, ["~> 0.12"])
    else
      s.add_dependency(%q<prawn>, ["< 3.0.0", ">= 1.1.0"])
      s.add_dependency(%q<pdf-inspector>, ["~> 1.2.1"])
      s.add_dependency(%q<rspec>, ["~> 3.5.0"])
      s.add_dependency(%q<rubocop>, ["~> 0.44.1"])
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<pdf-reader>, ["~> 1.4"])
      s.add_dependency(%q<simplecov>, ["~> 0.12"])
    end
  else
    s.add_dependency(%q<prawn>, ["< 3.0.0", ">= 1.1.0"])
    s.add_dependency(%q<pdf-inspector>, ["~> 1.2.1"])
    s.add_dependency(%q<rspec>, ["~> 3.5.0"])
    s.add_dependency(%q<rubocop>, ["~> 0.44.1"])
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<pdf-reader>, ["~> 1.4"])
    s.add_dependency(%q<simplecov>, ["~> 0.12"])
  end
end
