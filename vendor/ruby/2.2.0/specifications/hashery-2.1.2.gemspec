# -*- encoding: utf-8 -*-
# stub: hashery 2.1.2 ruby lib

Gem::Specification.new do |s|
  s.name = "hashery"
  s.version = "2.1.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Trans", "Kirk Haines", "Robert Klemme", "Jan Molic", "George Moschovitis", "Jeena Paradies", "Erik Veenstra"]
  s.date = "2016-05-01"
  s.description = "The Hashery is a tight collection of Hash-like classes. Included among its many offerings are the auto-sorting Dictionary class, the efficient LRUHash, the flexible OpenHash and the convenient KeyHash. Nearly every class is a subclass of the CRUDHash which defines a CRUD model on top of Ruby's standard Hash making it a snap to subclass and augment to fit any specific use case."
  s.email = ["transfire@gmail.com"]
  s.extra_rdoc_files = ["LICENSE.txt", "NOTICE.txt", "HISTORY.md", "README.md"]
  s.files = ["HISTORY.md", "LICENSE.txt", "NOTICE.txt", "README.md"]
  s.homepage = "http://rubyworks.github.com/hashery"
  s.licenses = ["BSD-2-Clause"]
  s.rubygems_version = "2.4.5.1"
  s.summary = "Facets-bread collection of Hash-like classes."

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<qed>, [">= 0"])
      s.add_development_dependency(%q<lemon>, [">= 0"])
      s.add_development_dependency(%q<rubytest-cli>, [">= 0"])
    else
      s.add_dependency(%q<qed>, [">= 0"])
      s.add_dependency(%q<lemon>, [">= 0"])
      s.add_dependency(%q<rubytest-cli>, [">= 0"])
    end
  else
    s.add_dependency(%q<qed>, [">= 0"])
    s.add_dependency(%q<lemon>, [">= 0"])
    s.add_dependency(%q<rubytest-cli>, [">= 0"])
  end
end
