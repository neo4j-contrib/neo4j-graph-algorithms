# encoding: utf-8
#
# Copyright October 2014, Jesse Doyle. All rights reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'spec_helper'

describe Prawn::Icon::Interface do
  let(:pdf) { create_pdf }

  describe '::icon' do
    context 'valid icon key' do
      context 'with options' do
        it 'should handle text options (size)' do
          pdf.icon 'fa-arrows', size: 60
          text = PDF::Inspector::Text.analyze(pdf.render)

          expect(text.font_settings.first[:size]).to eq(60)
        end
      end

      context 'inline_format: true' do
        it 'should handle text options (size)' do
          pdf.icon '<icon size="60">fa-arrows</icon>', inline_format: true
          text = PDF::Inspector::Text.analyze(pdf.render)

          expect(text.strings.first).to eq("\uf047")
          expect(text.font_settings.first[:size]).to eq(60.0)
        end

        it 'should be able to render on multiple documents' do
          pdf1 = create_pdf
          pdf2 = create_pdf
          pdf1.icon '<icon>fa-arrows</icon>', inline_format: true
          pdf2.icon '<icon>fa-arrows</icon>', inline_format: true
          text1 = PDF::Inspector::Text.analyze(pdf1.render)
          text2 = PDF::Inspector::Text.analyze(pdf2.render)

          expect(text1.strings.first).to eq("\uf047")
          expect(text2.strings.first).to eq("\uf047")
        end

        it 'renders the icon at the proper cursor position (#24)' do
          icon_text = '<icon>fa-info-circle</icon> icon here!'
          pdf.text 'Start'
          pdf.move_down 10
          pdf.text 'More'
          pdf.move_down 20
          icon = pdf.icon icon_text, inline_format: true
          pdf.move_down 30
          pdf.text 'End'

          expect(icon.at.first).to eq(0)
          expect(icon.at.last.round).to eq(734)
        end

        context 'with final_gap: false' do
          it 'renders the icon without a final gap' do
            icon = pdf.icon '<icon size="60">fa-arrows</icon>',
              inline_format: true,
              final_gap: false
            expect(icon.at.last.round).to eq(792)
          end
        end
      end

      context 'without options' do
        it 'should render an icon to document' do
          pdf.icon 'fa-arrows'
          text = PDF::Inspector::Text.analyze(pdf.render)

          expect(text.strings.first).to eq("\uf047")
        end
      end
    end

    context 'invalid icon key' do
      it 'should raise IconNotFound' do
        proc = Proc.new { pdf.icon 'fa-__INVALID' }

        expect(proc).to raise_error(Prawn::Icon::Errors::IconNotFound)
      end
    end

    context 'invalid specifier' do
      it 'should raise UnknownFont' do
        proc = Proc.new { pdf.icon '__INVALID__' }

        expect(proc).to raise_error(Prawn::Errors::UnknownFont)
      end
    end
  end

  describe '::make_icon' do
    context ':inline_format => false (default)' do
      it 'should return a Prawn::Icon instance' do
        icon = pdf.make_icon 'fa-arrows'

        expect(icon).to be_a(Prawn::Icon)
      end
    end

    context ':inline_format => true' do
      it 'should return a Prawn::::Text::Formatted::Box instance' do
        icon = pdf.make_icon '<icon>fa-arrows</icon>', inline_format: true

        expect(icon).to be_a(Prawn::Text::Formatted::Box)
      end
    end
  end

  describe '::inline_icon' do
    it 'should return a Prawn::Text::Formatted::Box instance' do
      icon = pdf.inline_icon '<icon>fa-arrows</icon>'

      expect(icon).to be_a(Prawn::Text::Formatted::Box)
    end
  end

  describe '::table_icon' do
    context 'inline_format: false (default)' do
      it 'should return a hash with font and content keys' do
        icon = pdf.table_icon 'fa-arrows'

        expect(icon).to be_a(Hash)
        expect(icon[:font]).to eq('fa')
        expect(icon[:content]).to eq("\uf047")
      end
    end

    context 'inline_format: true' do
      it 'should convert <icon> to <font> tags' do
        icon = pdf.table_icon '<icon>fa-user</icon>', inline_format: true

        expect(icon).to be_a(Hash)
        expect(icon[:content]).to eq('<font name="fa"></font>')
        expect(icon[:inline_format]).to be true
      end

      it 'should ignore all other tags' do
        a = ['<b>BOLD</b> <color rgb="0099FF">BLUE</color>', inline_format: true]
        icon = pdf.table_icon(*a)

        expect(icon).to be_a(Hash)
        expect(icon[:content]).to eq(a[0])
        expect(icon[:inline_format]).to be true
      end

      context 'multiple icons' do
        it 'should ignore any text not in an icon tag' do
          a = ['<icon>fa-user</icon> Some Text <icon>fi-laptop</icon>', inline_format: true]
          out = '<font name="fa"></font> Some Text <font name="fi"></font>'
          icon = pdf.table_icon(*a)

          expect(icon).to be_a(Hash)
          expect(icon[:content]).to eq(out)
          expect(icon[:inline_format]).to be true
        end
      end
    end
  end
end

describe Prawn::Icon do
  let(:pdf) { create_pdf }

  context 'FontAwesome' do
    it 'should render FontAwesome glyphs' do
      pdf.icon 'fa-user'
      text = PDF::Inspector::Text.analyze(pdf.render)

      expect(text.strings.first).to eq("")
    end
  end

  context 'Foundation Icons' do
    it 'should render Foundation glyphs' do
      pdf.icon 'fi-laptop'
      text = PDF::Inspector::Text.analyze(pdf.render)

      expect(text.strings.first).to eq("")
    end
  end

  context 'GitHub Octicons' do
    it 'should render GitHub Octicon glyphs' do
      pdf.icon 'octicon-logo-github'
      text = PDF::Inspector::Text.analyze(pdf.render)

      expect(text.strings.first).to eq("")
    end
  end

  context 'PaymentFont' do
    it 'should render PaymentFont glyphs' do
      pdf.icon 'pf-amazon'
      text = PDF::Inspector::Text.analyze(pdf.render)

      expect(text.strings.first).to eq("")
    end
  end
end
