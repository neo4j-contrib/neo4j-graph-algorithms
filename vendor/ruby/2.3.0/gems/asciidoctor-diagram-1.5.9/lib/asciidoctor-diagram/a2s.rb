require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'a2s/extension'

  block Asciidoctor::Diagram::AsciiToSvgBlockProcessor, :a2s
  block_macro Asciidoctor::Diagram::AsciiToSvgBlockMacroProcessor, :a2s
end
