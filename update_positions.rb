#!/usr/bin/ruby

require 'rubygems'
require 'vivo_web_api'
require '~/.passwords'

def find_positions()
  sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>

select ?ufid ?pos_uri ?dept_id ?start_year
where
{
  ?pos_uri rdf:type core:Position .
  ?pos_uri ufVivo:deptIDofPosition ?dept_id .
  optional {?pos_uri core:startYear ?start_year }
  ?pos_uri core:positionForPerson ?person .
  ?person ufVivo:ufid ?ufid
}
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
results = find_positions()
data = ""
results.each do |result|
  start_year = result[:start_year].nil? ? "" : result[:start_year].value
  data << "#{result[:ufid].value}\t#{result[:dept_id].value}\t#{result[:pos_uri]}\t#{start_year}\n"
end
File.open("vivo_positions.csv", "w") { |f| f.write(data)}
puts "End sparql query"
