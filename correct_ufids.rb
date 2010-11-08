#!/usr/bin/ruby -w

# Clean up ufids that are less than 8 characters. These ufids mistakenly had their leading 0's chopped off.

require 'rubygems'
require 'vivo_web_api'
require '~/.passwords.rb'

def generate_all_possible_removal_statements(subj, pred, obj, lang, datatype)
  objs = []
  objs << RDF::Literal.new(obj)
  if !lang.nil? 
    objs << RDF::Literal.new(obj, :language => lang)
  end
  if !datatype.nil? 
    objs << RDF::Literal.new(obj, :datatype => datatype)
  end
  if !lang.nil? && !datatype.nil? 
    objs << RDF::Literal.new(obj, :language => lang, :datatype => datatype)
  end

  statements = []
  objs.each do |obj|
    statements << RDF::Statement.new(subj, pred, obj)
  end
  return statements
end

sparql = <<-EOH
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
select ?person ?ufid
where 
{
  ?person ufVivo:ufid ?ufid 
}
EOH

hostname = ENV['vivo_hostname']
username = ENV['vivo_username']
password = ENV['vivo_password']

client = VivoWebApi::Client.new(hostname)
client.authenticate(username, password)
results = client.execute_sparql_select(username, password, sparql, 'RS_JSON')

add_data = []
remove_data = []
ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')
results.each do |result|
  ufid = result[:ufid].value
  if ufid.length < 8
    corrected_ufid = '0' * (8 - ufid.length) + ufid
    person_uri = RDF::URI.new(result[:person])
    add_data << RDF::Statement.new(person_uri, ufid_pred, RDF::Literal.new(corrected_ufid))
    string_datatype = RDF::URI.new("http://www.w3.org/2001/XMLSchema#string")

    remove_data = remove_data + generate_all_possible_removal_statements(person_uri, ufid_pred, ufid, nil, string_datatype)
  end
end

RDF::Writer.open('remove_ufids.nt') do |writer|
  remove_data.each do |datum|
    writer << datum
  end
end

RDF::Writer.open('add_ufids.nt') do |writer|
  add_data.each do |datum|
    writer << datum
  end
end
