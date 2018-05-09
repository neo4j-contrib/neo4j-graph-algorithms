require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'plantuml/extension'

  block Asciidoctor::Diagram::PlantUmlBlockProcessor, :plantuml
  block_macro Asciidoctor::Diagram::PlantUmlBlockMacroProcessor, :plantuml
end
