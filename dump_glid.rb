#!/usr/bin/ruby

require 'rubygems'
require 'dbi'
require '~/.passwords.rb'

# Dump out glid
def dump_glid_from_ps
  begin 
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

    sql = "select oprid as ufid, useridalias as glid from dbo.t_uf_pa_gl_acct"

    sth = dbh.execute(sql)
    data = []
    sth.fetch do |row|
      ufid = row[:ufid].nil? ? "" : row[:ufid].strip
      glid = row[:glid].nil? ? "" : row[:glid].strip
      data << { :ufid => ufid, :glid => glid }
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
def cache_ps_glid_data_in_mysql(records)
  begin 
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_glid"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_glid (ufid, glid) values (?, ?)"
    sth = dbh.prepare(insert_sql)
    records.each do |record|
      sth.execute(record[:ufid], record[:glid])
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

data = dump_glid_from_ps
cache_ps_glid_data_in_mysql(data)
