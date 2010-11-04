
#!/usr/bin/ruby

require 'dbi'
require '~/.passwords.rb'

# Dump out first name, last name, display name and middle name from people soft
dbh = DBI.connect('dbi:ODBC:erpprod', ENV['ps_glid'], ENV['ps_glid_pw'])

sql = <<-EOH
select 
  distinct uf_uuid1 as ufid, 
  uf_type_cd as type_cd 
from dbo.t_uf_dir_emp_stu_5 bar 
where 
  uf_type_cd = '197' 
  and bar.uf_uuid1 not in (
    select distinct foo.uf_uuid1 from dbo.t_uf_dir_emp_stu_5 foo where uf_type_cd = '192'
  )
EOH


sth = dbh.execute(sql)

results = ""
sth.fetch do |row|
  ufid = row[:ufid].nil? ? "" : row[:ufid].strip
  type_cd = row[:type_cd] # This comes in as a fixnum

  results << "#{ufid}\t#{type_cd}\n"
end
sth.finish

File.open("ps_197.csv", 'w') { |f| f.write(results) }
