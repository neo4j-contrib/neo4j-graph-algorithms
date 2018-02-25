# -*- encoding: utf-8 -*-
# stub: treetop 1.5.3 ruby lib

Gem::Specification.new do |s|
  s.name = "treetop"
  s.version = "1.5.3"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Nathan Sobo", "Clifford Heath"]
  s.autorequire = "treetop"
  s.date = "2014-03-21"
  s.email = "cliffordheath@gmail.com"
  s.executables = ["tt"]
  s.extra_rdoc_files = ["LICENSE", "README.md"]
  s.files = ["LICENSE", "README.md", "bin/tt"]
  s.homepage = "https://github.com/cjheath/treetop"
  s.licenses = ["MIT"]
  s.rubygems_version = "2.4.5.1"
  s.summary = "A Ruby-based text parsing and interpretation DSL"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<polyglot>, ["~> 0.3"])
      s.add_development_dependency(%q<jeweler>, ["~> 2.0"])
      s.add_development_dependency(%q<activesupport>, ["~> 4.0"])
      s.add_development_dependency(%q<i18n>, ["~> 0.6"])
      s.add_development_dependency(%q<rr>, ["~> 1.0"])
      s.add_development_dependency(%q<rspec>, ["~> 2"])
      s.add_development_dependency(%q<rake>, ["~> 10"])
    else
      s.add_dependency(%q<polyglot>, ["~> 0.3"])
      s.add_dependency(%q<jeweler>, ["~> 2.0"])
      s.add_dependency(%q<activesupport>, ["~> 4.0"])
      s.add_dependency(%q<i18n>, ["~> 0.6"])
      s.add_dependency(%q<rr>, ["~> 1.0"])
      s.add_dependency(%q<rspec>, ["~> 2"])
      s.add_dependency(%q<rake>, ["~> 10"])
    end
  else
    s.add_dependency(%q<polyglot>, ["~> 0.3"])
    s.add_dependency(%q<jeweler>, ["~> 2.0"])
    s.add_dependency(%q<activesupport>, ["~> 4.0"])
    s.add_dependency(%q<i18n>, ["~> 0.6"])
    s.add_dependency(%q<rr>, ["~> 1.0"])
    s.add_dependency(%q<rspec>, ["~> 2"])
    s.add_dependency(%q<rake>, ["~> 10"])
  end
end
