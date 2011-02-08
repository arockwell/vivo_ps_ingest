#!/usr/bin/ruby

require 'conf.rb'

def find_names()
  sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>

select ?person ?first_name ?last_name ?label ?ufid 
where 
{
  ?person ufVivo:ufid ?ufid .
  ?person foaf:firstName ?first_name . 
  ?person foaf:lastName ?last_name .
  ?person rdfs:label ?label
}
order by ?ufid
  EOH

  hostname = ENV['vivo_hostname']
  username = ENV['vivo_username']
  password = ENV['vivo_password']

  client = VivoWebApi::Client.new(hostname)
  client.authenticate(username, password)
  results = client.execute_sparql_select(username, password, sparql, 'RS_JSON')
  return results
end
  

puts "Start sparql query"
results = find_names()
data = ""
results.each do |result|
  data << "#{result[:ufid].value}\t#{result[:person]}\t#{result[:first_name].value}\t#{result[:last_name].value}\t#{result[:label].value}\n"
end
File.open('vivo_name_data.csv', 'w') { |f| f.write(data) }

puts "End sparql query"
system "join -t \"\t\" ps_faculty_names.csv vivo_name_data.csv > joined_name_data.csv"

add_statements = []
remove_statements = []

first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')

File.open('joined_name_data.csv').each do |line|
  (ufid, type, ps_value, uri, vivo_first_name, vivo_last_name, vivo_label) = line.chomp!.split("\t")
  uri = RDF::URI.new(uri)
  if type == '35'
    if vivo_first_name != ps_value
      add_statements << RDF::Statement.new(uri, first_name_pred, RDF::Literal.new(ps_value))
      remove_statements << RDF::Statement(uri, first_name_pred, RDF::Literal.new(vivo_first_name))
    end
  elsif type == '36'
    if vivo_last_name != ps_value
      add_statements << RDF::Statement.new(uri, last_name_pred, RDF::Literal.new(ps_value))
      remove_statements << RDF::Statement(uri, last_name_pred, RDF::Literal.new(vivo_last_name))
    end
  elsif type == '232'
    if vivo_label != ps_value
      add_statements << RDF::Statement.new(uri, label_pred, RDF::Literal.new(ps_value))
      remove_statements << RDF::Statement(uri, label_pred, RDF::Literal.new(vivo_label))
    end
  end
end


RDF::Writer.open('add_names.nt') do |writer|
  add_statements.each do |add_statement|
    writer << add_statement
  end
end

RDF::Writer.open('remove_names.nt') do |writer|
  remove_statements.each do |remove_statement|
    writer << remove_statement
  end
end
