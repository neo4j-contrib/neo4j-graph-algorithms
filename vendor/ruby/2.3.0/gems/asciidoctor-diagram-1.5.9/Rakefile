require 'rubygems/package_task'
require 'rspec/core/rake_task'

test = RSpec::Core::RakeTask.new(:test)

task :default => :test

spec = Gem::Specification.load('asciidoctor-diagram.gemspec')
Gem::PackageTask.new(spec) { |task| }
