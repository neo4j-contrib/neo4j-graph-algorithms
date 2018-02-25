# -*- encoding: utf-8 -*-
# stub: asciidoctor-pdf 1.5.0.alpha.16 ruby lib

Gem::Specification.new do |s|
  s.name = "asciidoctor-pdf"
  s.version = "1.5.0.alpha.16"

  s.required_rubygems_version = Gem::Requirement.new("> 1.3.1") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Dan Allen", "Sarah White"]
  s.date = "2017-07-31"
  s.description = "An extension for Asciidoctor that converts AsciiDoc documents to PDF using the Prawn PDF library.\n"
  s.email = "dan@opendevise.com"
  s.executables = ["asciidoctor-pdf"]
  s.extra_rdoc_files = ["CHANGELOG.adoc", "LICENSE.adoc", "NOTICE.adoc", "README.adoc"]
  s.files = ["CHANGELOG.adoc", "LICENSE.adoc", "NOTICE.adoc", "README.adoc", "bin/asciidoctor-pdf"]
  s.homepage = "https://github.com/asciidoctor/asciidoctor-pdf"
  s.licenses = ["MIT"]
  s.rdoc_options = ["--charset=UTF-8", "--title=\"Asciidoctor PDF\"", "--main=README.adoc", "-ri"]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3")
  s.rubygems_version = "2.4.5.1"
  s.summary = "Converts AsciiDoc documents to PDF using Prawn"

  s.installed_by_version = "2.4.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rake>, [">= 0"])
      s.add_runtime_dependency(%q<asciidoctor>, [">= 1.5.0"])
      s.add_runtime_dependency(%q<prawn>, ["< 2.3.0", ">= 1.3.0"])
      s.add_runtime_dependency(%q<prawn-table>, ["= 0.2.2"])
      s.add_runtime_dependency(%q<prawn-templates>, ["<= 0.1.1", ">= 0.0.3"])
      s.add_runtime_dependency(%q<prawn-svg>, ["< 0.28.0", ">= 0.21.0"])
      s.add_runtime_dependency(%q<prawn-icon>, ["= 1.3.0"])
      s.add_runtime_dependency(%q<safe_yaml>, ["~> 1.0.4"])
      s.add_runtime_dependency(%q<thread_safe>, ["~> 0.3.6"])
      s.add_runtime_dependency(%q<treetop>, ["= 1.5.3"])
    else
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<asciidoctor>, [">= 1.5.0"])
      s.add_dependency(%q<prawn>, ["< 2.3.0", ">= 1.3.0"])
      s.add_dependency(%q<prawn-table>, ["= 0.2.2"])
      s.add_dependency(%q<prawn-templates>, ["<= 0.1.1", ">= 0.0.3"])
      s.add_dependency(%q<prawn-svg>, ["< 0.28.0", ">= 0.21.0"])
      s.add_dependency(%q<prawn-icon>, ["= 1.3.0"])
      s.add_dependency(%q<safe_yaml>, ["~> 1.0.4"])
      s.add_dependency(%q<thread_safe>, ["~> 0.3.6"])
      s.add_dependency(%q<treetop>, ["= 1.5.3"])
    end
  else
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<asciidoctor>, [">= 1.5.0"])
    s.add_dependency(%q<prawn>, ["< 2.3.0", ">= 1.3.0"])
    s.add_dependency(%q<prawn-table>, ["= 0.2.2"])
    s.add_dependency(%q<prawn-templates>, ["<= 0.1.1", ">= 0.0.3"])
    s.add_dependency(%q<prawn-svg>, ["< 0.28.0", ">= 0.21.0"])
    s.add_dependency(%q<prawn-icon>, ["= 1.3.0"])
    s.add_dependency(%q<safe_yaml>, ["~> 1.0.4"])
    s.add_dependency(%q<thread_safe>, ["~> 0.3.6"])
    s.add_dependency(%q<treetop>, ["= 1.5.3"])
  end
end
