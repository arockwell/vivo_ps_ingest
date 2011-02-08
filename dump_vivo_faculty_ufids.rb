#!/usr/bin/ruby

require 'conf.rb'

def find_faculty_ufids()
  sparql = <<-EOH
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
select ?person ?ufid 
where 
{
  ?person ufVivo:ufid ?ufid .
  ?person rdf:type core:FacultyMember
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
  
def cache_vivo_results_in_db(results)
  begin
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from vivo_faculty_ufids"
    dbh.do(clear_table_sql)

    insert_sql = "insert into vivo_faculty_ufids (uri, ufid) values (?, ?)"
    sth = dbh.prepare(insert_sql)
    results.each do |result|
      sth.execute(result[:person], result[:ufid].value)
    end
    sth.finish
    dbh.commit
  rescue DBI::DatabaseError => e
    puts "An error occurred"
    puts "Error code:    #{e.err}"
    puts "Error message: #{e.errstr}"
  ensure 
    dbh.disconnect if dbh
  end
end

ufid = find_faculty_ufids
cache_vivo_results_in_db(ufid)
