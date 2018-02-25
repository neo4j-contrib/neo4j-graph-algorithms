# -*- encoding: utf-8 -*-
# stub: Ascii85 1.0.3 ruby lib

Gem::Specification.new do |s|
  s.name = "Ascii85"
  s.version = "1.0.3"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Johannes Holzfu\u{df}"]
  s.date = "2018-01-25"
  s.description = "Ascii85 provides methods to encode/decode Adobe's binary-to-text encoding of the same name."
  s.email = "johannes@holzfuss.name"
  s.executables = ["ascii85"]
  s.extra_rdoc_files = ["README.md", "LICENSE"]
  s.files = ["LICENSE", "README.md", "bin/ascii85"]
  s.homepage = "https://github.com/DataWraith/ascii85gem/"
  s.licenses = ["MIT"]
  s.rubygems_version = "2.4.5.1"
  s.summary = "Ascii85 encoder/decoder"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<bundler>, [">= 1.0.0"])
      s.add_development_dependency(%q<minitest>, [">= 2.6.0"])
      s.add_development_dependency(%q<rake>, [">= 0.9.2"])
    else
      s.add_dependency(%q<bundler>, [">= 1.0.0"])
      s.add_dependency(%q<minitest>, [">= 2.6.0"])
      s.add_dependency(%q<rake>, [">= 0.9.2"])
    end
  else
    s.add_dependency(%q<bundler>, [">= 1.0.0"])
    s.add_dependency(%q<minitest>, [">= 2.6.0"])
    s.add_dependency(%q<rake>, [">= 0.9.2"])
  end
end
