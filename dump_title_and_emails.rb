#!/usr/bin/ruby

require 'conf.rb'

#Dump information from dbo.t_uf_dir_emp_stu_1. Key peieces of info are working title, work email, and ufid
dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

sql = <<-EOH
select
  distinct uf_identifier,
  uf_email,
  uf_work_title
from dbo.t_uf_dir_emp_stu_1
order by uf_identifier
EOH

sth = dbh.execute(sql)

stu1 = ""
sth.fetch do |row|
  # Remove whitespace
  uf_identifier = row[:uf_identifier].nil? ? "" : row[:uf_identifier].strip
  uf_email = row[:uf_email].nil? ? "" : row[:uf_email].strip
  uf_work_title = row[:uf_work_title].nil? ? "" : row[:uf_work_title].strip
  stu1 << "#{uf_identifier}\t#{uf_email}\t#{uf_work_title}\n"
end
sth.finish

File.open("ps_title_and_emails.csv", "w") { |f| f.write(stu1) }
