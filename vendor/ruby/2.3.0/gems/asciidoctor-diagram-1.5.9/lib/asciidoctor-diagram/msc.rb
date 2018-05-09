require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'msc/extension'

  block Asciidoctor::Diagram::MscBlockProcessor, :msc
  block_macro Asciidoctor::Diagram::MscBlockMacroProcessor, :msc
end
