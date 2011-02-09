#!/usr/bin/ruby

require 'conf.rb'

def print_privacy_flag_status(ufids)
  begin 
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])

    sql = "select uf_identifier, uf_security_flg, uf_protect_flg from dbo.t_uf_dir_emp_stu_1 where uf_identifier = '#{ufids[0]} '"
    ufids.each do |ufid|
      sql = sql + " or uf_identifier = '#{ufid}'"
    end
    puts sql

    sth = dbh.execute(sql)
    data = []
    sth.fetch do |row|
      ufid = row[:uf_identifier].nil? ? "" : row[:uf_identifier].strip
      uf_privacy_flg = row[:uf_security_flg].nil? ? "" : row[:uf_security_flg].strip
      uf_protect_flg = row[:uf_protect_flg].nil? ? "" : row[:uf_protect_flg].strip
      puts "#{ufid}\t#{uf_privacy_flg}\t#{uf_protect_flg}"
    end
    sth.finish
  rescue DBI::DatabaseError => e
    puts "An error occurred"
    puts "Error code:    #{e.err}"
    puts "Error message: #{e.errstr}"
  ensure
    dbh.disconnect if dbh
  end
end

ufids = []
puts "Getting ufids"
File.open('ufids.txt').each do |ufid|
  ufids << ufid.chomp
end

puts "Found #{ufids.size}"
print_privacy_flag_status(ufids)
