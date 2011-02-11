#!/usr/bin/ruby -w

require 'conf.rb'

# Dump privacy flags
def dump_ps_privacy_flags
  dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

  sql = <<-EOH
select
  distinct uf_identifier as ufid,
  uf_security_flg as security_flag,
  uf_protect_flg as protect_flag
from dbo.t_uf_dir_emp_stu_1
  EOH

  sth = dbh.execute(sql)

  results = []
  sth.fetch do |row|
    ufid = row[:ufid].nil? ? "" : row[:ufid].strip
    security_flag = row[:security_flag].nil? ? "" : row[:security_flag].strip
    protect_flag = row[:protect_flag].nil? ? "" : row[:protect_flag].strip

    results << { :ufid => ufid, :security_flag => security_flag, :protect_flag => protect_flag }
  end
  sth.finish

  return results
end

def cache_ps_privacy_flags_in_mysql(results)
  begin
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from ps_privacy_flags"
    dbh.do(clear_table_sql)

    insert_sql = "insert into ps_privacy_flags (ufid, security_flag, protect_flag) values (?, ?, ?)"
    sth = dbh.prepare(insert_sql)
    results.each do |result|
      sth.execute(result[:ufid], result[:protect_flag], result[:security_flag])
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

results = dump_ps_privacy_flags
cache_ps_privacy_flags_in_mysql(results)
