#!/usr/bin/ruby

require 'rubygems'
require 'vivo_web_api'
require '~/.passwords.rb'

def find_phone_numbers()
  sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>

select ?ufid ?person ?work_phone ?work_fax
where 
{
  ?person ufVivo:ufid ?ufid .
  ?person core:workPhone ?work_phone .
  optional { ?person core:workFax ?work_fax }
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
results = find_phone_numbers()
data = ""
results.each do |result|
  work_phone = result[:work_phone].nil? ? "" : result[:work_phone].value
  work_phone.gsub!("\n", "")
  work_fax = result[:work_fax].nil? ? "" : result[:work_fax].value
  work_fax.gsub!("\n", "")
  data << "#{result[:ufid].value}\t#{result[:person]}\t#{work_phone}\t#{work_fax}\n"
end
File.open('vivo_phone_numbers.csv', 'w') { |f| f.write(data) }
puts "End sparql query"


system "join -t \"\t\" ps_phone_numbers.csv vivo_phone_numbers.csv > joined_phone_numbers.csv"

add_statements = []
remove_statements = []
work_phone_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workPhone')
work_fax_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workFax')
File.open('joined_phone_numbers.csv').each do |line|
  (ufid, type, ps_value, uri, vivo_work_phone, vivo_work_fax) = line.chomp!.split("\t")
  uri = RDF::URI.new(uri)
  if type == '10'
    if vivo_work_phone != ps_value
      add_statements << RDF::Statement.new(uri, work_phone_pred, RDF::Literal.new(ps_value))
      remove_statements << RDF::Statement(uri, work_phone_pred, RDF::Literal.new(vivo_work_phone))
    end
  elsif type == '11'
    if vivo_work_fax != ps_value
      add_statements << RDF::Statement.new(uri, work_fax_pred, RDF::Literal.new(ps_value))
      remove_statements << RDF::Statement(uri, work_fax_pred, RDF::Literal.new(vivo_work_fax))
    end
  end
end

RDF::Writer.open('add_phone_numbers.nt') do |writer|
  add_statements.each do |add_statement|
    writer << add_statement
  end
end

RDF::Writer.open('remove_phone_numbers.nt') do |writer|
  remove_statements.each do |remove_statement|
    writer << remove_statement
  end
end
