#!/usr/bin/ruby

require 'dbi'
require '~/.passwords.rb'

# Dump work phone, work fax from ps
def dump_ps_phone_numbers
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

  results = []
  sth.fetch do |row|
    ufid = row[:ufid].nil? ? "" : row[:ufid].strip
    type_cd = row[:type_cd] # This comes in as a fixnum
    area_code = row[:area_code].nil? ? "" : row[:area_code].strip
    phone_number = row[:phone_number].nil? ? "" : row[:phone_number].strip
    extension = row[:extension].nil? ? "" : row[:extension].strip
    phone = "#{area_code}.#{phone_number.slice(0..2)}.#{phone_number.slice(3..6)}"
    phone = extension == "" ? phone : phone + " x" + extension

    results << { :ufid => ufid, :type_cd => type_cd, :phone_number => phone }
  end
  sth.finish

  return results
end

def cache_ps_phone_numbers_in_mysql(results)
  begin
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_phone_numbers"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_phone_numbers (ufid, type_cd, phone_number) values (?, ?, ?)"
    sth = dbh.prepare(insert_sql)
    results.each do |result|
      sth.execute(result[:ufid], result[:type_cd], result[:phone_number])
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

results = dump_ps_phone_numbers
cache_ps_phone_numbers_in_mysql(results)



