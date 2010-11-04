#!/usr/bin/ruby

require 'rubygems'
require 'dbi'
require 'vivo_web_api'
require '~/.passwords'

def find_names()
  sparql = <<-EOH
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>

select ?person ?ufid 
where 
{
  ?person ufVivo:ufid ?ufid .
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

    clear_table_sql = "delete from vivo_names"
    dbh.do(clear_table_sql)

    insert_sql = "insert into vivo_ufids (uri, ufid) values (?, ?)"
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
