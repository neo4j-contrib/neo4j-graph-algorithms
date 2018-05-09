require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'vega/extension'

  block Asciidoctor::Diagram::VegaBlockProcessor, :vega
  block_macro Asciidoctor::Diagram::VegaBlockMacroProcessor, :vega

  block Asciidoctor::Diagram::VegaBlockProcessor, :vegalite
  block_macro Asciidoctor::Diagram::VegaBlockMacroProcessor, :vegalite
end
