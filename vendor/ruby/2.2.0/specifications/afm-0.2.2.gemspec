# -*- encoding: utf-8 -*-
# stub: afm 0.2.2 ruby lib

Gem::Specification.new do |s|
  s.name = "afm"
  s.version = "0.2.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Jan Krutisch"]
  s.date = "2014-06-19"
  s.description = "a simple library to read afm files and use the data conveniently"
  s.email = "jan@krutisch.de"
  s.extra_rdoc_files = ["LICENSE", "README.rdoc"]
  s.files = ["LICENSE", "README.rdoc"]
  s.homepage = "http://github.com/halfbyte/afm"
  s.licenses = ["MIT"]
  s.rubygems_version = "2.4.5.1"
  s.summary = "reading Adobe Font Metrics (afm) files"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rake>, ["~> 10.3"])
      s.add_development_dependency(%q<rdoc>, ["~> 4.1"])
      s.add_development_dependency(%q<minitest>, ["~> 5.3"])
    else
      s.add_dependency(%q<rake>, ["~> 10.3"])
      s.add_dependency(%q<rdoc>, ["~> 4.1"])
      s.add_dependency(%q<minitest>, ["~> 5.3"])
    end
  else
    s.add_dependency(%q<rake>, ["~> 10.3"])
    s.add_dependency(%q<rdoc>, ["~> 4.1"])
    s.add_dependency(%q<minitest>, ["~> 5.3"])
  end
end
