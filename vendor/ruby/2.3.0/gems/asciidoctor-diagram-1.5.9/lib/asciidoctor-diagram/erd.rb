require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'erd/extension'
  block Asciidoctor::Diagram::ErdBlockProcessor, :erd
  block_macro Asciidoctor::Diagram::ErdBlockMacroProcessor, :erd
end
