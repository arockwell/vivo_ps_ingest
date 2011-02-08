#!/usr/bin/ruby

require 'conf.rb'

#Dump information about positions from stu5 and per_ufau

def dump_positions_in_ps(dbh)
  sql = <<-EOH
select distinct
  per_ufau.uf_uuid as ufid, 
  stu5.ps_deptid as dept_id, 
  per_ufau.uf_job_long as job_title, 
  year(stu5.uf_begin_ts) as start_year 
from dbo.t_UF_PER_UFAU per_ufau 
  join dbo.t_uf_dir_emp_stu_5 stu5 on per_ufau.uf_uuid = stu5.uf_uuid1 
where 
  stu5.uf_type_cd = '192'
order by ufid
  EOH

  sth = dbh.execute(sql)
  results = []
  sth.fetch do |row|
    # Remove whitespace
    ufid = row[:ufid].nil? ? "" : row[:ufid].strip
    dept_id = row[:dept_id].nil? ? "" : row[:dept_id].strip
    job_title = row[:job_title].nil? ? "" : row[:job_title].strip
    start_year = row[:start_year].nil? ? "" : row[:start_year] # of type fixnum

    results << {:ufid => ufid, :dept_id => dept_id, :job_title => job_title, :start_year => start_year }
  end
  sth.finish
  return results
end

def cache_ps_positions(dbh, positions)
  clear_table_sql = "delete from ps_positions"
  dbh.do(clear_table_sql)
  
  insert_sql = "insert into ps_positions (ufid, dept_id, job_title, start_year) values (?, ?, ?, ?)"
  sth = dbh.prepare(insert_sql)
  positions.each do |pos|
    sth.execute(pos[:ufid], pos[:dept_id], pos[:job_title], pos[:start_year])
  end

  sth.finish
  dbh.commit
end

begin
  dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])
  positions = dump_positions_in_ps(dbh)

  mysql_dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
  cache_ps_positions(mysql_dbh, positions)
rescue DBI::DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
ensure 
  dbh.disconnect if dbh
end
