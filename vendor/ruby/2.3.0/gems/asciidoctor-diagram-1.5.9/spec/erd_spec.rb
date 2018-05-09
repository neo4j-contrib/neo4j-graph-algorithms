require_relative 'test_helper'

code = <<-eos
title {label: "nfldb Entity-Relationship diagram (condensed)", size: "20"}

# Entities

[player] {bgcolor: "#d0e0d0"}
  *player_id {label: "varchar, not null"}
  full_name {label: "varchar, null"}
  team {label: "varchar, not null"}
  position {label: "player_pos, not null"}
  status {label: "player_status, not null"}

[team] {bgcolor: "#d0e0d0"}
  *team_id {label: "varchar, not null"}
  city {label: "varchar, not null"}
  name {label: "varchar, not null"}

[game] {bgcolor: "#ececfc"}
  *gsis_id {label: "gameid, not null"}
  start_time {label: "utctime, not null"}
  week {label: "usmallint, not null"}
  season_year {label: "usmallint, not null"}
  season_type {label: "season_phase, not null"}
  finished {label: "boolean, not null"}
  home_team {label: "varchar, not null"}
  home_score {label: "usmallint, not null"}
  away_team {label: "varchar, not null"}
  away_score {label: "usmallint, not null"}

[drive] {bgcolor: "#ececfc"}
  *+gsis_id {label: "gameid, not null"}
  *drive_id {label: "usmallint, not null"}
  start_field {label: "field_pos, null"}
  start_time {label: "game_time, not null"}
  end_field {label: "field_pos, null"}
  end_time {label: "game_time, not null"}
  pos_team {label: "varchar, not null"}
  pos_time {label: "pos_period, null"}

[play] {bgcolor: "#ececfc"}
  *+gsis_id {label: "gameid, not null"}
  *+drive_id {label: "usmallint, not null"}
  *play_id {label: "usmallint, not null"}
  time {label: "game_time, not null"}
  pos_team {label: "varchar, not null"}
  yardline {label: "field_pos, null"}
  down {label: "smallint, null"}
  yards_to_go {label: "smallint, null"}

[play_player] {bgcolor: "#ececfc"}
  *+gsis_id {label: "gameid, not null"}
  *+drive_id {label: "usmallint, not null"}
  *+play_id {label: "usmallint, not null"}
  *+player_id {label: "varchar, not null"}
  team {label: "varchar, not null"}

[meta] {bgcolor: "#fcecec"}
  version {label: "smallint, null"}
  season_type {label: "season_phase, null"}
  season_year {label: "usmallint, null"}
  week {label: "usmallint, null"}

# Relationships

player      *--1 team
game        *--1 team {label: "home"}
game        *--1 team {label: "away"}
drive       *--1 team
play        *--1 team
play_player *--1 team

game        1--* drive
game        1--* play
game        1--* play_player

drive       1--* play
drive       1--* play_player

play        1--* play_player

player      1--* play_player
eos

describe Asciidoctor::Diagram::ErdBlockMacroProcessor, :broken_on_travis, :broken_on_windows do
  it "should generate PNG images when format is set to 'png'" do
    File.write('erd.txt', code)

    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

erd::erd.txt[format="png"]
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.png$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it "should generate SVG images when format is set to 'svg'" do
    File.write('erd.txt', code)

    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

erd::erd.txt[format="svg"]
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.svg/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end
end

describe Asciidoctor::Diagram::ErdBlockProcessor, :broken_on_travis, :broken_on_windows do
  it "should generate PNG images when format is set to 'png'" do
    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

[erd, format="png"]
----
#{code}
----
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.png$/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it "should generate SVG images when format is set to 'svg'" do
    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

[erd, format="svg"]
----
#{code}
----
    eos

    d = load_asciidoc doc
    expect(d).to_not be_nil

    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil

    expect(b.content_model).to eq :empty

    target = b.attributes['target']
    expect(target).to_not be_nil
    expect(target).to match(/\.svg/)
    expect(File.exist?(target)).to be true

    expect(b.attributes['width']).to_not be_nil
    expect(b.attributes['height']).to_not be_nil
  end

  it "should raise an error when when format is set to an invalid value" do
    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

[erd, format="foobar"]
----
----
    eos

    expect { load_asciidoc doc }.to raise_error(/support.*format/i)
  end

  it "should not regenerate images when source has not changed" do
    File.write('erd.txt', code)

    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

erd::erd.txt

[erd, format="png"]
----
#{code}
----
    eos

    d = load_asciidoc doc
    b = d.find { |bl| bl.context == :image }
    expect(b).to_not be_nil
    target = b.attributes['target']
    mtime1 = File.mtime(target)

    sleep 1

    d = load_asciidoc doc

    mtime2 = File.mtime(target)

    expect(mtime2).to eq mtime1
  end

  it "should handle two block macros with the same source" do
    File.write('erd.txt', code)

    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

erd::erd.txt[]
erd::erd.txt[]
    eos

    load_asciidoc doc
    expect(File.exist?('erd.png')).to be true
  end

  it "should respect target attribute in block macros" do
    File.write('erd.txt', code)

    doc = <<-eos
= Hello, Erd!
Doc Writer <doc@example.com>

== First Section

erd::erd.txt["foobar"]
erd::erd.txt["foobaz"]
    eos

    load_asciidoc doc
    expect(File.exist?('foobar.png')).to be true
    expect(File.exist?('foobaz.png')).to be true
    expect(File.exist?('erd.png')).to be false
  end
end