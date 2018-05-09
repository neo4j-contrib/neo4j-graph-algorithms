require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'syntrax/extension'

  block Asciidoctor::Diagram::SyntraxBlockProcessor, :syntrax
  block_macro Asciidoctor::Diagram::SyntraxBlockMacroProcessor, :syntrax
end
