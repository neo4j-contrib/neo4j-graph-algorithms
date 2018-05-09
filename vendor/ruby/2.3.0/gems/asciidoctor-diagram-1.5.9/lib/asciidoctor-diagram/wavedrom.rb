require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'wavedrom/extension'

  block Asciidoctor::Diagram::WavedromBlockProcessor, :wavedrom
  block_macro Asciidoctor::Diagram::WavedromBlockMacroProcessor, :wavedrom
end
