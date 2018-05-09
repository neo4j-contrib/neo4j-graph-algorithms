require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'svgbob/extension'

  block Asciidoctor::Diagram::SvgBobBlockProcessor, :svgbob
  block_macro Asciidoctor::Diagram::SvgBobBlockMacroProcessor, :svgbob
end
