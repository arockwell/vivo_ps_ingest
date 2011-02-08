#!/usr/bin/ruby 

require 'conf.rb'

def find_positions()
  sparql = <<-EOH
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX core: <http://vivoweb.org/ontology/core#>

select ?pos ?org ?dept_id 
where 
{
  ?pos ufVivo:deptIDofPosition ?dept_id .
  ?pos core:positionInOrganization ?org
}
order by ?dept_id
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

    clear_table_sql = "delete from vivo_positions"
    dbh.do(clear_table_sql)

    insert_sql = "insert into vivo_positions (uri, org_uri, dept_id) values (?, ?, ?)"
    sth = dbh.prepare(insert_sql)
    results.each do |result|
      sth.execute(result[:pos], result[:org], result[:dept_id].value)
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

positions = find_positions
cache_vivo_results_in_db(positions)
