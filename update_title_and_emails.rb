#!/usr/bin/ruby

require 'rubygems'
require 'vivo_web_api'
require '~/.passwords'

def find_title_and_emails()
  sparql = <<-EOH

PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>

select ?person ?ufid ?workEmail ?preferredTitle
where 
{ 
    ?person ufVivo:ufid ?ufid .
    ?person core:workEmail ?workEmail .
    ?person vitro:moniker ?preferredTitle
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
  data << "#{result[:ufid].value}\t#{result[:person]}\t#{result[:workEmail].value}\t#{result[:preferredTitle].value}\n"
end
File.open("vivo_title_and_emails.csv", "w") { |f| f.write(data)}
puts "End sparql query"

system "join -t \"\t\" ps_title_and_emails.csv vivo_title_and_emails.csv > joined_email_and_titles.csv"

File.open("joined_email_and_titles.csv").each do |line|
  (ufid, ps_work_email, ps_preferred_title, uri, vivo_work_email, vivo_preferred_title) = line.chomp!.split("\t")
  if ps_work_email != vivo_work_email
    puts "Work Email Mismatch! URI: #{uri} Ufid: #{ufid} Old Work Email: #{vivo_work_email} New Work Email: #{ps_work_email}\n"
  end
  if ps_preferred_title != vivo_preferred_title
    puts "Preferred Title Mismatch! URI: #{uri} Ufid: #{ufid} Old Preferred Title: #{vivo_preferred_title} New Preferred Title: #{ps_preferred_title}\n"
  end
end
