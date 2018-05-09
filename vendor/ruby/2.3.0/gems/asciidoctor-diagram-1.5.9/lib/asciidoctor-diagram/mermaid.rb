require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'mermaid/extension'

  block Asciidoctor::Diagram::MermaidBlockProcessor, :mermaid
  block_macro Asciidoctor::Diagram::MermaidBlockMacroProcessor, :mermaid
end
