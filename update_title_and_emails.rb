#!/usr/bin/ruby

require 'conf.rb'

def find_title_and_emails()
  sparql = <<-EOH

PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>

select ?person ?ufid ?workEmail ?moniker
where 
{ 
    ?person ufVivo:ufid ?ufid .
    ?person core:workEmail ?workEmail .
    ?person vitro:moniker ?moniker
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
results = find_title_and_emails()
data = ""
results.each do |result|
  data << "#{ufid}\t#{result[:person]}\t#{result[:workEmail].value}\t#{result[:moniker].value}\n"
end
File.open("vivo_title_and_emails.csv", "w") { |f| f.write(data)}
puts "End sparql query"

system "join -t \"\t\" ps_title_and_emails.csv vivo_title_and_emails.csv > joined_email_and_titles.csv"

add_statements = []
remove_statements = []

work_email_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workEmail')
moniker_pred = RDF::URI.new('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#moniker')

File.open("joined_email_and_titles.csv").each do |line|
  (ufid, ps_work_email, ps_moniker, uri, vivo_work_email, vivo_moniker) = line.chomp!.split("\t")
  uri = RDF::URI.new(uri)
  if ps_work_email != vivo_work_email
    add_statements << RDF::Statement.new(uri, work_email_pred, RDF::Literal.new(ps_work_email))
    remove_statements << RDF::Statement(uri, work_email_pred, RDF::Literal.new(vivo_work_email))
  end
  if ps_moniker != vivo_moniker
    add_statements << RDF::Statement.new(uri, moniker_pred, RDF::Literal.new(ps_moniker))
    remove_statements << RDF::Statement(uri, moniker_pred, RDF::Literal.new(vivo_moniker))
  end
end

RDF::Writer.open('add_title_and_emails.nt') do |writer|
  add_statements.each do |add_statement|
    writer << add_statement
  end
end

RDF::Writer.open('remove_title_and_emails.nt') do |writer|
  remove_statements.each do |remove_statement|
    writer << remove_statement
  end
end
