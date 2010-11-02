#!/usr/bin/ruby

require 'rubygems'
require 'vivo_web_api'
require '~/.passwords'

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

File.open('joined_name_data.csv').each do |line|
  (ufid, type, ps_value, uri, vivo_first_name, vivo_last_name, vivo_label) = line.chomp!.split("\t")
  if type == '35'
    if vivo_first_name != ps_value
      puts "First Name Mismatch! URI: #{uri} Ufid: #{ufid} First Name Mismatch!  Old: '#{vivo_first_name}'\t New: '#{ps_value}'"
    end
  elsif type == '36'
    if vivo_last_name != ps_value
      puts "Last Name Mismatch! URI: #{uri} Ufid: #{ufid} Old: '#{vivo_last_name}'\t New: '#{ps_value}'"
    end
  elsif type == '232'
    if vivo_label != ps_value
      puts "Display Name Mismatch! URI: #{uri} Ufid: #{ufid} Old: '#{vivo_label}'\t New: '#{ps_value}'"
    end
  end
end
