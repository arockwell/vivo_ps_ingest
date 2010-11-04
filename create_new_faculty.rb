#!/usr/bin/ruby

require 'rubygems'
require 'dbi'
require 'rdf/raptor'
require 'rdf/ntriples'
require '~/.passwords.rb'


def create_blank_nodes(dbh)
  # Find all ufids not in our vivo
  sql = <<-EOH
select distinct ps.ufid from psIngestDev.ps_names ps
where
not exists (
  select vivo.ufid from psIngestDev.vivo_names vivo where ps.ufid = vivo.ufid
)
  EOH

  sth = dbh.execute(sql)

  ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')
  blank_node_people = []
  sth.fetch do |row|
    # ignore ufids with leading 0's since they are not in vivo correctly
    if row[:ufid][0..0] != "0"
      blank_node = RDF::Node.new
      blank_node_people << { :uri => blank_node, :ufid => row[:ufid] }
    end
  end

  clear_table_sql = "delete from vivo_blank_node_people"
  dbh.do(clear_table_sql)

  insert_sql = "insert into vivo_blank_node_people (uri, ufid) values (?, ?)"
  sth = dbh.prepare(insert_sql)
  blank_node_people.each do |blank_person|
    sth.execute(blank_person[:uri].id, blank_person[:ufid])
  end
  sth.finish
  dbh.commit
end


def generate_name_rdf(dbh)
  join_names_to_blank_nodes = <<-EOH
select ps.ufid, ps.type_cd, ps.name_text, vivo.uri 
from psIngestDev.ps_names ps, vivo_blank_node_people vivo 
where ps.ufid = vivo.ufid
  EOH

  first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
  last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
  label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')
  sth = dbh.execute(join_names_to_blank_nodes)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    if row[:type_cd] == 35
      data << RDF::Statement(uri, first_name_pred, row[:name_text])
    elsif row[:type_cd] == 36
      data << RDF::Statement(uri, last_name_pred, row[:name_text])
    elsif row[:type_cd] == 232
      data << RDF::Statement(uri, label_pred, row[:name_text])
    end
  end
  return data
end

def generate_ufid_rdf(dbh)
  sql = <<-EOH
select uri, ufid from vivo_blank_node_people
  EOH

  ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    data << RDF::Statement(uri, ufid_pred, row[:ufid])
  end
  return data
end

def generate_type_rdf(dbh)
  sql = <<-EOH
select uri, ufid from vivo_blank_node_people
  EOH

  type_pred = RDF::URI.new('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  person_types = [ RDF::URI.new('http://www.w3.org/2002/07/owl#Thing'),
    RDF::URI.new('http://xmlns.com/foaf/0.1/Person'), 
    RDF::URI.new('http://vivoweb.org/ontology/core#FacultyMember') ]

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    person_types.each do |person_type|
      data << RDF::Statement(uri, type_pred, person_type)
    end
  end
  return data
end

begin
  dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
  create_blank_nodes(dbh)
  name_rdf = generate_name_rdf(dbh)
  ufid_rdf = generate_ufid_rdf(dbh)
  type_rdf = generate_type_rdf(dbh)

  RDF::Writer.open('new_faculty_names.nt') do |writer|
    name_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_ufids.nt') do |writer|
    ufid_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_types.nt') do |writer|
    type_rdf.each do |datum|
      writer << datum
    end
  end
rescue DBI::DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
ensure 
  dbh.disconnect if dbh
end
