require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'plantuml/extension'

  block Asciidoctor::Diagram::SaltBlockProcessor, :salt
  block_macro Asciidoctor::Diagram::SaltBlockMacroProcessor, :salt
end
