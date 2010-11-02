#!/usr/bin/ruby

require 'dbi'
require '~/.passwords.rb'

# Dump out first name, last name, display name and middle name from people soft
dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

sql = <<-EOH
select 
  distinct stu4.uf_uuid as ufid,
  stu4.uf_type_cd as type_cd,
  stu4.uf_name_txt name_text
from dbo.t_uf_dir_emp_stu_4 stu4 join dbo.t_uf_dir_emp_stu_5 stu5 on (stu4.uf_uuid = stu5.uf_uuid1)
where (stu4.uf_type_cd = '232' or stu4.uf_type_cd = '35' or stu4.uf_type_cd = '36' or stu4.uf_type_cd='37')
and (stu5.uf_type_cd = '192' or stu5.uf_type_cd = '219')
EOH

sth = dbh.execute(sql)

stu4 = ""
sth.fetch do |row|
  ufid = row[:ufid].nil? ? "" : row[:ufid].strip
  type_cd = row[:type_cd] # This comes in as a fixnum
  name_text = row[:name_text].nil? ? "" : row[:name_text].strip
  stu4 << "#{ufid}\t#{type_cd}\t#{name_text}\n"
end
sth.finish

File.open("ps_faculty_names.csv", 'w') { |f| f.write(stu4) }
