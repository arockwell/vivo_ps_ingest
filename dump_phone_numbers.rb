#!/usr/bin/ruby

require 'dbi'
require '~/.passwords.rb'

# Dump out first name, last name, display name and middle name from people soft
dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

sql = <<-EOH
select
  distinct uf_uuid as ufid,
  uf_type_cd as type_cd,
  uf_area_cd as area_code,
  uf_phone_no as phone_number,
  uf_phon_extn as extension
from dbo.t_uf_dir_emp_stu_2
where
  uf_type_cd = '10' 
  or uf_type_cd = '11'
order by uf_uuid
EOH

sth = dbh.execute(sql)

stu2 = ""
sth.fetch do |row|
  ufid = row[:ufid].nil? ? "" : row[:ufid].strip
  type_cd = row[:type_cd] # This comes in as a fixnum
  area_code = row[:area_code].nil? ? "" : row[:area_code].strip
  phone_number = row[:phone_number].nil? ? "" : row[:phone_number].strip
  extension = row[:extension].nil? ? "" : row[:extension].strip
  phone = "#{area_code}.#{phone_number.slice(0..2)}.#{phone_number.slice(3..6)}"
  phone = extension == "" ? phone : phone + " x" + extension

  stu2 << "#{ufid}\t#{type_cd}\t#{phone}\n"
end
sth.finish

File.open("ps_phone_numbers.csv", 'w') { |f| f.write(stu2) }

