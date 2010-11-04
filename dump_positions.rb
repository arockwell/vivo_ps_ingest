#!/usr/bin/ruby

#Dump information about positions from stu5 and per_ufau

require 'dbi'
require '~/.passwords.rb'

dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

sql = <<-EOH
select 
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
results = ""
sth.fetch do |row|
  # Remove whitespace
  ufid = row[:ufid].nil? ? "" : row[:ufid].strip
  dept_id = row[:dept_id].nil? ? "" : row[:dept_id].strip
  job_title = row[:job_title].nil? ? "" : row[:job_title].strip
  start_year = row[:start_year].nil? ? "" : row[:start_year] # of type fixnum

  results << "#{ufid}\t#{dept_id}\t#{job_title}\t#{start_year}\n"
end
sth.finish

File.open("ps_positions.csv", "w") { |f| f.write(results) }
