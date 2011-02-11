#!/usr/bin/ruby

require 'conf.rb'

#Dump information about positions from stu5 and per_ufau

def dump_positions_in_ps(dbh)
  sql = <<-EOH
select distinct
  stu5.uf_uuid1 as ufid, 
  stu5.ps_deptid as dept_id
from dbo.t_uf_dir_emp_stu_5 stu5
  EOH

  sth = dbh.execute(sql)
  results = []
  sth.fetch do |row|
    # Remove whitespace
    ufid = row[:ufid].nil? ? "" : row[:ufid].strip
    dept_id = row[:dept_id].nil? ? "" : row[:dept_id].strip

    results << {:ufid => ufid, :dept_id => dept_id }
  end
  sth.finish
  return results
end

def cache_ps_positions(dbh, positions)
  clear_table_sql = "delete from ps_positions"
  dbh.do(clear_table_sql)
  
  insert_sql = "insert into ps_positions (ufid, dept_id) values (?, ?)"
  sth = dbh.prepare(insert_sql)
  positions.each do |pos|
    sth.execute(pos[:ufid], pos[:dept_id])
  end

  sth.finish
  dbh.commit
end

begin
  puts "dump positions"
  dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])
  positions = dump_positions_in_ps(dbh)

  mysql_dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
  puts "cache positions in vivo"
  cache_ps_positions(mysql_dbh, positions)
rescue DBI::DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
ensure 
  dbh.disconnect if dbh
end
