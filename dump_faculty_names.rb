#!/usr/bin/ruby

require 'rubygems'
require 'dbi'
require '~/.passwords.rb'

# Dump out first name, last name, display name and middle name from people soft
def dump_names_from_ps
  begin 
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

    data = []
    sth.fetch do |row|
      ufid = row[:ufid].nil? ? "" : row[:ufid].strip
      type_cd = row[:type_cd] # This comes in as a fixnum
      name_text = row[:name_text].nil? ? "" : row[:name_text].strip
      data << { :ufid => ufid, :type_cd => type_cd, :name_text => name_text }
    end
    sth.finish
  rescue DBI::DatabaseError => e
    puts "An error occurred"
    puts "Error code:    #{e.err}"
    puts "Error message: #{e.errstr}"
  ensure
    dbh.disconnect if dbh
  end
  return data
end

# Cache this in our mysql database
def cache_ps_name_data_in_mysql(records)
  begin 
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_names"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_names (ufid, type_cd, name_text) values (?, ?, ?)"
    sth = dbh.prepare(insert_sql)
    records.each do |record|
      sth.execute(record[:ufid], record[:type_cd], record[:name_text])
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

data = dump_names_from_ps
cache_ps_name_data_in_mysql(data)
