#!/usr/bin/ruby

require 'rubygems'
require 'dbi'
require '~/.passwords.rb'

# Dump out work_email
def dump_work_email_from_ps
  begin 
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

    sql = "select uf_identifier as ufid, uf_email as work_email from dbo.t_uf_dir_emp_stu_1"

    sth = dbh.execute(sql)
    data = []
    sth.fetch do |row|
      ufid = row[:ufid].nil? ? "" : row[:ufid].strip
      work_email = row[:work_email].nil? ? "" : row[:work_email].strip
      data << { :ufid => ufid, :work_email => work_email }
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
def cache_ps_work_email_data_in_mysql(records)
  begin 
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_work_email"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_work_email (ufid, work_email) values (?, ?)"
    sth = dbh.prepare(insert_sql)
    records.each do |record|
      sth.execute(record[:ufid], record[:work_email])
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

data = dump_work_email_from_ps
cache_ps_work_email_data_in_mysql(data)
