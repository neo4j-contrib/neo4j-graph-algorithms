require_relative 'extensions'

Asciidoctor::Extensions.register do
  require_relative 'blockdiag/extension'

  ['BlockDiag', 'SeqDiag', 'ActDiag', 'NwDiag', 'RackDiag', 'PacketDiag'].each do |tool|
    block ::Asciidoctor::Diagram.const_get("#{tool}BlockProcessor"), tool.downcase.to_sym
    block_macro ::Asciidoctor::Diagram.const_get("#{tool}BlockMacroProcessor"), tool.downcase.to_sym
  end
end
