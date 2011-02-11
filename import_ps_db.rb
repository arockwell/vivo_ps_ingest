#!/usr/bin/ruby

require 'conf.rb'

require 'pp'

def extract_ps_table(sql, column_map)
  begin
    dbh = DBI.connect(ENV['ps_odbc_connection'], ENV['ps_glid'], ENV['ps_glid_pw'])
    sth = dbh.execute(sql)

    data = []
    sth.fetch do |row|
      result = {}
      column_map.keys.each do |ps_column|
        value = row[ps_column]
        if !value.nil? 
          if value.respond_to?(:strip)
            value = value.strip
          end
        else
          value = ""
        end
        result[column_map[ps_column]] = value
      end
      data << result
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

def insert_data(table_name, data)
  columns = data[0].keys
  pp columns
  column_sql = ""
  columns[0..columns.size - 2].each do |column|
    column_sql = column_sql + column + ", "
  end
  column_sql = column_sql + columns[columns.size - 1]

  q_marks = "?," * (columns.size - 1)
  q_marks = q_marks + "?"
  insert_sql = "insert into #{table_name} (#{column_sql}) values (#{q_marks})" 
  begin 
    dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

    clear_table_sql = "delete from #{table_name}"
    dbh.do(clear_table_sql)

    sth = dbh.prepare(insert_sql)
    puts insert_sql
    data.each do |record|
      i = 1
      columns.each do |column|
        value = record[column]
        sth.bind_param(i, value)
        i = i + 1
      end
      sth.execute
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

table_map = {
  'dbo.t_uf_dir_emp_stu_1' => {
    :local_name => 'ps_employee_records',
    :column_map => {
      'uf_identifier' => 'ufid',
      'uf_work_title' => 'work_title',
      'uf_email' => 'uf_email',
      'uf_protect_flg' => 'protect_flag',
      'uf_security_flg' => 'security_flag'
    }
  },
  'dbo.t_uf_dir_emp_stu_2' => {
    :local_name => 'ps_phone_numbers',
    :column_map => {
      'uf_uuid' => 'ufid',
      'uf_type_cd' => 'type_cd',
      'uf_area_cd' => 'area_code',
      'uf_phone_no' => 'phone_number',
      'uf_phon_extn' => 'extension'
    }
  },
  'dbo.t_uf_dir_emp_stu_4' => {
    :local_name => 'ps_names',
    :column_map => {
      'uf_uuid' => 'ufid',
      'uf_type_cd' => 'type_cd',
      'uf_name_txt' => 'name_text',
    }
  },
  'dbo.t_uf_dir_emp_stu_5' => {
    :local_name => 'ps_directory_relationships',
    :column_map => {
      'uf_uuid1' => 'ufid',
      'ps_deptid' => 'dept_id',
      'uf_type_cd' => 'type_cd',
      'uf_begin_ts' => 'begin_date'
    }
  },
  'dbo.t_uf_per_ufau' => {
    :local_name => 'ps_position_titles',
    :column_map => {
      'uf_uuid' => 'ufid',
      'uf_ps_deptid' => 'dept_id',
      'uf_job_long' => 'job_title'
    }
  },
  'dbo.t_uf_pa_gl_acct' => {
    :local_name => 'ps_glid',
    :column_map => {
      'oprid' => 'ufid',
      'useridalias' => 'glid'
    },
  },
}

table_map.keys.each do |ps_table_name|
  ps_columns = table_map[ps_table_name][:column_map].keys
  column_sql = ""
  ps_columns[0..ps_columns.size - 2].each do |column|
    column_sql = column_sql + column + ", "
  end
  column_sql = column_sql + ps_columns[ps_columns.size - 1]

  sql = "select distinct #{column_sql} from #{ps_table_name}"
  puts sql
  before_time = Time.now
  results = extract_ps_table(sql, table_map[ps_table_name][:column_map] )
  after_time = Time.now
  puts "Duration: #{after_time - before_time}s"

  before_time = Time.now
  insert_data(table_map[ps_table_name][:local_name], results)
  after_time = Time.now
  puts "Duration: #{after_time - before_time}s"
end
