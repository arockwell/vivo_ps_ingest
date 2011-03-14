#!/usr/bin/ruby

require '../conf.rb'

def find_orgs()
  sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

select distinct ?org ?dept_id
where
{
  ?org rdf:type foaf:Organization .
  ?org ufVivo:deptID ?dept_id_with_optional_type
  let (?dept_id := str(?dept_id_with_optional_type))
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

def cache_vivo_results_in_db(dbh, results)
  clear_table_sql = "delete from vivo_orgs"
  dbh.do(clear_table_sql)

  insert_sql = "insert into vivo_orgs (uri, dept_id) values (?, ?)"
  sth = dbh.prepare(insert_sql)
  results.each do |result|
    sth.execute(result[:org], result[:dept_id].value)
  end
  sth.finish
  dbh.commit
end

begin
  dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])
  orgs = find_orgs

  mysql_dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
  cache_vivo_results_in_db(mysql_dbh, orgs)
rescue DBI::DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
ensure 
  dbh.disconnect if dbh
end
