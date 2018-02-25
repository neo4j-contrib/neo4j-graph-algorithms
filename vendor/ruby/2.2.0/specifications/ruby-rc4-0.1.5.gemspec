# -*- encoding: utf-8 -*-
# stub: ruby-rc4 0.1.5 ruby lib

Gem::Specification.new do |s|
  s.name = "ruby-rc4"
  s.version = "0.1.5"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Caige Nichols"]
  s.date = "2012-01-25"
  s.email = "caigesn@gmail.com"
  s.extra_rdoc_files = ["README.md"]
  s.files = ["README.md"]
  s.homepage = "http://www.caigenichols.com/"
  s.rdoc_options = ["--main", "README.md"]
  s.rubyforge_project = "ruby-rc4"
  s.rubygems_version = "2.4.5.1"
  s.summary = "RubyRC4 is a pure Ruby implementation of the RC4 algorithm."

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rspec>, [">= 0"])
    else
      s.add_dependency(%q<rspec>, [">= 0"])
    end
  else
    s.add_dependency(%q<rspec>, [">= 0"])
  end
end
