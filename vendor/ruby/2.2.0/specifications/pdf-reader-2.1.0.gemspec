# -*- encoding: utf-8 -*-
# stub: pdf-reader 2.1.0 ruby lib

Gem::Specification.new do |s|
  s.name = "pdf-reader"
  s.version = "2.1.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["James Healy"]
  s.date = "2018-02-15"
  s.description = "The PDF::Reader library implements a PDF parser conforming as much as possible to the PDF specification from Adobe"
  s.email = ["jimmy@deefa.com"]
  s.executables = ["pdf_object", "pdf_text", "pdf_callbacks"]
  s.extra_rdoc_files = ["README.md", "TODO", "CHANGELOG", "MIT-LICENSE"]
  s.files = ["CHANGELOG", "MIT-LICENSE", "README.md", "TODO", "bin/pdf_callbacks", "bin/pdf_object", "bin/pdf_text"]
  s.homepage = "http://github.com/yob/pdf-reader"
  s.licenses = ["MIT"]
  s.rdoc_options = ["--title", "PDF::Reader Documentation", "--main", "README.md", "-q"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3")
  s.rubygems_version = "2.4.5.1"
  s.summary = "A library for accessing the content of PDF files"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rake>, [">= 0"])
      s.add_development_dependency(%q<rspec>, ["~> 3.5"])
      s.add_development_dependency(%q<cane>, ["~> 3.0"])
      s.add_development_dependency(%q<morecane>, ["~> 0.2"])
      s.add_development_dependency(%q<ir_b>, [">= 0"])
      s.add_development_dependency(%q<rdoc>, [">= 0"])
      s.add_runtime_dependency(%q<Ascii85>, ["~> 1.0.0"])
      s.add_runtime_dependency(%q<ruby-rc4>, [">= 0"])
      s.add_runtime_dependency(%q<hashery>, ["~> 2.0"])
      s.add_runtime_dependency(%q<ttfunk>, [">= 0"])
      s.add_runtime_dependency(%q<afm>, ["~> 0.2.1"])
    else
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<rspec>, ["~> 3.5"])
      s.add_dependency(%q<cane>, ["~> 3.0"])
      s.add_dependency(%q<morecane>, ["~> 0.2"])
      s.add_dependency(%q<ir_b>, [">= 0"])
      s.add_dependency(%q<rdoc>, [">= 0"])
      s.add_dependency(%q<Ascii85>, ["~> 1.0.0"])
      s.add_dependency(%q<ruby-rc4>, [">= 0"])
      s.add_dependency(%q<hashery>, ["~> 2.0"])
      s.add_dependency(%q<ttfunk>, [">= 0"])
      s.add_dependency(%q<afm>, ["~> 0.2.1"])
    end
  else
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<rspec>, ["~> 3.5"])
    s.add_dependency(%q<cane>, ["~> 3.0"])
    s.add_dependency(%q<morecane>, ["~> 0.2"])
    s.add_dependency(%q<ir_b>, [">= 0"])
    s.add_dependency(%q<rdoc>, [">= 0"])
    s.add_dependency(%q<Ascii85>, ["~> 1.0.0"])
    s.add_dependency(%q<ruby-rc4>, [">= 0"])
    s.add_dependency(%q<hashery>, ["~> 2.0"])
    s.add_dependency(%q<ttfunk>, [">= 0"])
    s.add_dependency(%q<afm>, ["~> 0.2.1"])
  end
end
