require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'shaape/extension'

  block Asciidoctor::Diagram::ShaapeBlockProcessor, :shaape
  block_macro Asciidoctor::Diagram::ShaapeBlockMacroProcessor, :shaape
end
