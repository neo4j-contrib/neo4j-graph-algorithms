# All example code may be executed by calling `rake legend`

require_relative '../lib/prawn/icon'
require_relative 'example_helper'

Prawn::Document.generate('fontawesome.pdf') do
  deja_path = File.join \
    Prawn::Icon::Base::FONTDIR, 'DejaVuSans.ttf'

  font_families.update({
    'deja' => { normal: deja_path }
  })

  font('deja')

  icons = icon_keys(self, 'fa')
  required_pages = number_of_pages(self, 'fa')

  define_grid(columns: 6, rows: 12, gutter: 16)

  sub_header = 'FontAwesome'
  link = 'http://fontawesome.io/icons/'
  page_header sub_header, link

  first_page_icons icons do |icon_key|
    # Just call the +icon+ method and pass in an icon key
    icon icon_key, size: 20, align: :center
  end

  start_new_page

  page_icons icons, required_pages do |icon_key|
    icon icon_key, size: 20, align: :center
  end
end
