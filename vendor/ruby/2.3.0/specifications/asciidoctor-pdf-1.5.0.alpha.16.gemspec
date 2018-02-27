# -*- encoding: utf-8 -*-
# stub: asciidoctor-pdf 1.5.0.alpha.16 ruby lib

Gem::Specification.new do |s|
  s.name = "asciidoctor-pdf".freeze
  s.version = "1.5.0.alpha.16"

  s.required_rubygems_version = Gem::Requirement.new("> 1.3.1".freeze) if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib".freeze]
  s.authors = ["Dan Allen".freeze, "Sarah White".freeze]
  s.date = "2017-07-31"
  s.description = "An extension for Asciidoctor that converts AsciiDoc documents to PDF using the Prawn PDF library.\n".freeze
  s.email = "dan@opendevise.com".freeze
  s.executables = ["asciidoctor-pdf".freeze]
  s.extra_rdoc_files = ["CHANGELOG.adoc".freeze, "LICENSE.adoc".freeze, "NOTICE.adoc".freeze, "README.adoc".freeze]
  s.files = ["CHANGELOG.adoc".freeze, "LICENSE.adoc".freeze, "NOTICE.adoc".freeze, "README.adoc".freeze, "bin/asciidoctor-pdf".freeze]
  s.homepage = "https://github.com/asciidoctor/asciidoctor-pdf".freeze
  s.licenses = ["MIT".freeze]
  s.rdoc_options = ["--charset=UTF-8".freeze, "--title=\"Asciidoctor PDF\"".freeze, "--main=README.adoc".freeze, "-ri".freeze]
  s.required_ruby_version = Gem::Requirement.new(">= 1.9.3".freeze)
  s.rubygems_version = "2.5.2".freeze
  s.summary = "Converts AsciiDoc documents to PDF using Prawn".freeze

  s.installed_by_version = "2.5.2" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rake>.freeze, [">= 0"])
      s.add_runtime_dependency(%q<asciidoctor>.freeze, [">= 1.5.0"])
      s.add_runtime_dependency(%q<prawn>.freeze, ["< 2.3.0", ">= 1.3.0"])
      s.add_runtime_dependency(%q<prawn-table>.freeze, ["= 0.2.2"])
      s.add_runtime_dependency(%q<prawn-templates>.freeze, ["<= 0.1.1", ">= 0.0.3"])
      s.add_runtime_dependency(%q<prawn-svg>.freeze, ["< 0.28.0", ">= 0.21.0"])
      s.add_runtime_dependency(%q<prawn-icon>.freeze, ["= 1.3.0"])
      s.add_runtime_dependency(%q<safe_yaml>.freeze, ["~> 1.0.4"])
      s.add_runtime_dependency(%q<thread_safe>.freeze, ["~> 0.3.6"])
      s.add_runtime_dependency(%q<treetop>.freeze, ["= 1.5.3"])
    else
      s.add_dependency(%q<rake>.freeze, [">= 0"])
      s.add_dependency(%q<asciidoctor>.freeze, [">= 1.5.0"])
      s.add_dependency(%q<prawn>.freeze, ["< 2.3.0", ">= 1.3.0"])
      s.add_dependency(%q<prawn-table>.freeze, ["= 0.2.2"])
      s.add_dependency(%q<prawn-templates>.freeze, ["<= 0.1.1", ">= 0.0.3"])
      s.add_dependency(%q<prawn-svg>.freeze, ["< 0.28.0", ">= 0.21.0"])
      s.add_dependency(%q<prawn-icon>.freeze, ["= 1.3.0"])
      s.add_dependency(%q<safe_yaml>.freeze, ["~> 1.0.4"])
      s.add_dependency(%q<thread_safe>.freeze, ["~> 0.3.6"])
      s.add_dependency(%q<treetop>.freeze, ["= 1.5.3"])
    end
  else
    s.add_dependency(%q<rake>.freeze, [">= 0"])
    s.add_dependency(%q<asciidoctor>.freeze, [">= 1.5.0"])
    s.add_dependency(%q<prawn>.freeze, ["< 2.3.0", ">= 1.3.0"])
    s.add_dependency(%q<prawn-table>.freeze, ["= 0.2.2"])
    s.add_dependency(%q<prawn-templates>.freeze, ["<= 0.1.1", ">= 0.0.3"])
    s.add_dependency(%q<prawn-svg>.freeze, ["< 0.28.0", ">= 0.21.0"])
    s.add_dependency(%q<prawn-icon>.freeze, ["= 1.3.0"])
    s.add_dependency(%q<safe_yaml>.freeze, ["~> 1.0.4"])
    s.add_dependency(%q<thread_safe>.freeze, ["~> 0.3.6"])
    s.add_dependency(%q<treetop>.freeze, ["= 1.5.3"])
  end
end
