#!/usr/bin/ruby

require 'conf.rb'

def dump_ps_types
  begin 
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

    sql = <<-EOH
select 
  distinct stu5.uf_uuid1 as ufid,
  stu5.uf_type_cd as type_cd,
from dbo.t_uf_dir_emp_stu_5 stu5 
where (stu5.uf_type_cd = '192' or stu4.uf_type_cd = '219')
    EOH

    sth = dbh.execute(sql)

    data = []
    sth.fetch do |row|
      ufid = row[:ufid].nil? ? "" : row[:ufid].strip
      type_cd = row[:type_cd] # This comes in as a fixnum
      data << { :ufid => ufid, :type_cd => type_cd }
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

    clear_table_sql = "delete from ps_types"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_names (ufid, type_cd) values (?, ?, ?)"
    sth = dbh.prepare(insert_sql)
    records.each do |record|
      sth.execute(record[:ufid], record[:type_cd])
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

puts "Begin collecting ps data"
data = dump_names_from_ps
puts "Begin inserting data into mysql"
cache_ps_name_data_in_mysql(data)
