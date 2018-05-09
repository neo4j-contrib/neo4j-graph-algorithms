require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'graphviz/extension'
  block Asciidoctor::Diagram::GraphvizBlockProcessor, :graphviz
  block_macro Asciidoctor::Diagram::GraphvizBlockMacroProcessor, :graphviz
end
