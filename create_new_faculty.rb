#!/usr/bin/ruby

require 'conf.rb'

# As a prereq for running this script, run the following:
# dump_faculty_names.rb
# dump_vivo_ufids.rb
# dump_vivo_orgs.rb
# dump_positions.rb

def create_blank_nodes(dbh)
  # Find all ufids not in our vivo
  sql = <<-EOH
select distinct ps.ufid from psIngestDev.ps_names ps_names join psIngestDev.ps_types ps_types
 on (ps_names.ufid = ps_types.ufid)
where
not exists (
  select vivo.ufid from psIngestDev.vivo_ufids vivo where ps_names.ufid = vivo.ufid
)
and (ps_types.type_cd = '192' or ps_types.type_cd = '219')
and (ps_privacy_flags.security_flag = 'N' and ps_privacy_flags.protect_flg = 'N')
  EOH

  sth = dbh.execute(sql)

  ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')
  blank_node_people = []
  sth.fetch do |row|
    blank_node = RDF::Node.new
    blank_node_people << { :uri => blank_node, :ufid => row[:ufid] }
  end

  clear_table_sql = "delete from vivo_blank_node_people"
  dbh.do(clear_table_sql)

  insert_sql = "insert into vivo_blank_node_people (uri, ufid) values (?, ?)"
  sth = dbh.prepare(insert_sql)
  blank_node_people.each do |blank_person|
    sth.execute(blank_person[:uri].id, blank_person[:ufid])
  end
  sth.finish
  dbh.commit
end


def generate_name_rdf(dbh)
  join_names_to_blank_nodes = <<-EOH
select ps.ufid, ps.type_cd, ps.name_text, vivo.uri 
from psIngestDev.ps_names ps, vivo_blank_node_people vivo 
where ps.ufid = vivo.ufid
  EOH

  first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
  last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
  middle_name_pred = RDF::URI.new('http://vivoweb.org/ontology/core#middleName') 
  label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')
  prefix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/prefixName')
  suffix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/suffixName')

  sth = dbh.execute(join_names_to_blank_nodes)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    if row[:type_cd] == 35
      data << RDF::Statement(uri, first_name_pred, row[:name_text])
    elsif row[:type_cd] == 36
      data << RDF::Statement(uri, last_name_pred, row[:name_text])
    elsif row[:type_cd] == 37 && row[:type_cd] != ""
      data << RDF::Statement(uri, middle_name_pred, row[:name_text])
    elsif row[:type_cd] == 38
      data << RDF::Statement(uri, prefix_name_pred, row[:name_text])
    elsif row[:type_cd] == 39
      data << RDF::Statement(uri, suffix_name_pred, row[:name_text])
    elsif row[:type_cd] == 232
      data << RDF::Statement(uri, label_pred, row[:name_text])
    end
  end
  return data
end

def generate_phone_number_rdf(dbh)
  sql = <<-EOH
select vivo.uri, ps.ufid, ps.type_cd, ps.phone_number 
from psIngestDev.ps_phone_numbers ps, vivo_blank_node_people vivo
where ps.ufid = vivo.ufid
  EOH

  work_phone_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workPhone')
  work_fax_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workFax')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    if row[:type_cd] == 10 
      data << RDF::Statement(uri, work_phone_pred, row[:phone_number])
    elsif row[:type_cd] == 11
      data << RDF::Statement(uri, work_fax_pred, row[:phone_number])
    end
  end
  return data
end

def generate_work_email_rdf(dbh)
  sql = <<-EOH
select vivo.uri, ps.ufid, ps.work_email 
from psIngestDev.ps_work_email ps, vivo_blank_node_people vivo
where ps.ufid = vivo.ufid
  EOH

  work_email_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workEmail')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    if row[:work_email] != ""
      data << RDF::Statement(uri, work_email_pred, row[:work_email])
    end
  end
  return data
end

def generate_work_title_rdf(dbh)
  sql = <<-EOH
select vivo.uri, ps.ufid, ps.work_title 
from psIngestDev.ps_work_title ps, vivo_blank_node_people vivo
where ps.ufid = vivo.ufid
  EOH

  hr_job_title = RDF::URI.new('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#moniker')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    if row[:work_title] != ""
      data << RDF::Statement(uri, hr_job_title, row[:work_title])
    end
  end
  return data
end

def generate_ufid_rdf(dbh)
  sql = <<-EOH
select uri, ufid from vivo_blank_node_people
  EOH

  ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    data << RDF::Statement(uri, ufid_pred, row[:ufid])
  end
  return data
end

def generate_glid_rdf(dbh)
  sql = <<-EOH
select vivo.uri, ps.ufid, ps.glid
from psIngestDev.ps_glid ps, vivo_blank_node_people vivo
where ps.ufid = vivo.ufid
  EOH

  gatorlink_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    data << RDF::Statement(uri, gatorlink_pred, row[:glid])
  end
  return data
end

def generate_type_rdf(dbh)
  sql = <<-EOH
select uri, ufid from vivo_blank_node_people
  EOH

  type_pred = RDF::URI.new('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  person_types = [ RDF::URI.new('http://www.w3.org/2002/07/owl#Thing'),
    RDF::URI.new('http://xmlns.com/foaf/0.1/Person'), 
    RDF::URI.new('http://vivoweb.org/ontology/core#FacultyMember') ]

  harvested_by_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy')
  date_harvested_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/dateHarvested')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    uri = RDF::Node.new
    uri.id = RDF::URI.new(row[:uri])
    person_types.each do |person_type|
      data << RDF::Statement(uri, type_pred, person_type)
    end
    # set harvester properties
    data << RDF::Statement.new(uri, harvested_by_pred, 'PeopleSoft-Library Harvester')
    data << RDF::Statement.new(uri, date_harvested_pred, Time.now.localtime.strftime("%Y-%m-%d"))
  end


  return data
end

def generate_pos_rdf(dbh)
  sql = <<-EOH
select blank.uri as person_uri, vivo_orgs.uri as org_uri, ps.dept_id, ps.ufid, ps.job_title, ps.start_year
from psIngestDev.ps_positions ps, psIngestDev.vivo_orgs vivo_orgs, vivo_blank_node_people blank
where ps.dept_id = vivo_orgs.dept_id and ps.ufid = blank.ufid
  EOH
  
  type_pred = RDF::URI.new('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  person_in_position_pred = RDF::URI.new('http://vivoweb.org/ontology/core#personInPosition') 
  position_type_pred = RDF::URI.new('http://vivoweb.org/ontology/core#Position')
  faculty_position_type_pred = RDF::URI.new('http://vivoweb.org/ontology/core#FacultyPosition')
  dependent_resource_type_pred = RDF::URI.new('http://vivoweb.org/ontology/core#DependentResource')

  position_label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')
  hr_job_title_pred = RDF::URI.new('http://vivoweb.org/ontology/core#hrJobTitle')
  position_for_person_pred = RDF::URI.new('http://vivoweb.org/ontology/core#positionForPerson')
  position_in_organization_pred = RDF::URI.new('http://vivoweb.org/ontology/core#positionInOrganization')
  organization_for_position_pred = RDF::URI.new('http://vivoweb.org/ontology/core#organizationForPosition')
  dept_id_of_position_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/deptIDofPosition')
  start_year_pred = RDF::URI.new('http://vivoweb.org/ontology/core#startYear')

  harvested_by_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy')
  date_harvested_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/dateHarvested')

  sth = dbh.execute(sql)
  data = []
  sth.fetch do |row|
    # This is hack to make a bnode with a specific id
    person_uri = RDF::Node.new
    person_uri.id = RDF::URI.new(row[:person_uri])
    pos_uri = RDF::Node.new
    org_uri = RDF::URI.new(row[:org_uri])

    # connect person to position
    data << RDF::Statement.new(person_uri, person_in_position_pred, pos_uri)

    # set position properties
    data << RDF::Statement.new(pos_uri, type_pred, position_type_pred)
    data << RDF::Statement.new(pos_uri, type_pred, faculty_position_type_pred)
    data << RDF::Statement.new(pos_uri, type_pred, dependent_resource_type_pred)
    data << RDF::Statement.new(pos_uri, position_label_pred, row[:job_title])
    data << RDF::Statement.new(pos_uri, hr_job_title_pred, row[:job_title])
    data << RDF::Statement.new(pos_uri, dept_id_of_position_pred, row[:dept_id])
    data << RDF::Statement.new(pos_uri, start_year_pred, row[:start_year])

    # connect pos -> person and pos -> org
    data << RDF::Statement.new(pos_uri, position_for_person_pred, person_uri)
    data << RDF::Statement.new(pos_uri, position_in_organization_pred, org_uri)

    # connect org -> pos
    data << RDF::Statement.new(org_uri, organization_for_position_pred, pos_uri)

    # set harvester properties
    data << RDF::Statement.new(pos_uri, harvested_by_pred, 'PeopleSoft-Library Harvester')
    data << RDF::Statement.new(pos_uri, date_harvested_pred, Time.now.localtime.strftime("%Y-%m-%d"))
  end
  return data
end

begin
  dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
  create_blank_nodes(dbh)
  name_rdf = generate_name_rdf(dbh)
  phone_number_rdf = generate_phone_number_rdf(dbh)
  work_email_rdf = generate_work_email_rdf(dbh)
  work_title_rdf = generate_work_title_rdf(dbh)
  
  ufid_rdf = generate_ufid_rdf(dbh)
  glid_rdf = generate_glid_rdf(dbh)

  type_rdf = generate_type_rdf(dbh)

  pos_rdf = generate_pos_rdf(dbh)

  RDF::Writer.open('new_faculty_names.nt') do |writer|
    name_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_phone_numbers.nt') do |writer|
    phone_number_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_work_email.nt') do |writer|
    work_email_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_work_title.nt') do |writer|
    work_title_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_ufids.nt') do |writer|
    ufid_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_glid.nt') do |writer|
    glid_rdf.each do |datum|
      writer << datum
    end
  end

  RDF::Writer.open('new_faculty_types.nt') do |writer|
    type_rdf.each do |datum|
      writer << datum
    end
  end
  RDF::Writer.open('new_faculty_pos.nt') do |writer|
    pos_rdf.each do |datum|
      writer << datum
    end
  end
rescue DBI::DatabaseError => e
  puts "An error occurred"
  puts "Error code:    #{e.err}"
  puts "Error message: #{e.errstr}"
ensure 
  dbh.disconnect if dbh
end
