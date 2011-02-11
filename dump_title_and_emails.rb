#!/usr/bin/ruby

require 'conf.rb'


#Dump information from dbo.t_uf_dir_emp_stu_1. Key peieces of info are working title, work email, and ufid
def dump_work_title_from_ps
  begin
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

    sql = <<-EOH
select
  distinct uf_identifier as ufid,
  uf_work_title as work_title
from dbo.t_uf_dir_emp_stu_1
order by uf_identifier
    EOH

    sth = dbh.execute(sql)
    data = []
    sth.fetch do |row|
      # Remove whitespace
      uf_identifier = row[:ufid].nil? ? "" : row[:ufid].strip
      uf_work_title = row[:work_title].nil? ? "" : row[:work_title].strip

      data << { :ufid => uf_identifier, :work_title => uf_work_title }
    end
    sth.finish
  rescue DB::DatabaseError => e
    puts "An error occurred"
    puts "Error code:    #{e.err}"
    puts "Error message: #{e.errstr}"
  ensure
    dbh.disconnect if dbh
  end
  return data
end

def cache_work_title_in_vivo(records)
  begin 
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_work_title"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_work_title (ufid, work_title) values (?, ?)"
    sth = dbh.prepare(insert_sql)
    records.each do |record|
      sth.execute(record[:ufid], record[:work_title])
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

records = dump_work_title_from_ps
cache_work_title_in_vivo(records)
