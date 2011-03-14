#!/usr/bin/ruby

require '../conf.rb'

def create_blank_nodes(dbh)
  # Find all ufids not in our vivo
  sql = <<-EOH
select distinct ps_directory_relationships.ufid from
  psIngestDev.ps_directory_relationships ps_directory_relationships,
  psIngestDev.ps_employee_records ps_employee_records 
where
  not exists (
    select vivo.ufid from psIngestDev.vivo_ufids vivo where ps_directory_relationships.ufid = vivo.ufid
  )
  and (ps_directory_relationships.type_cd = '192' or ps_directory_relationships.type_cd = '219')
  and (ps_employee_records.security_flag = 'N' and ps_employee_records.protect_flag = 'N')
  and (ps_directory_relationships.ufid = ps_employee_records.ufid)
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
  return blank_node_people
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
select blank.uri as person_uri, vivo_orgs.uri as org_uri, ps.dept_id, ps.ufid, ps_position_titles.job_title, year(ps.begin_date) as start_year
from 
  psIngestDev.ps_directory_relationships ps, 
  psIngestDev.ps_position_titles ps_position_titles, 
  psIngestDev.vivo_orgs vivo_orgs, 
  vivo_blank_node_people blank
where ps.dept_id = vivo_orgs.dept_id and ps.ufid = blank.ufid and ps_position_titles.ufid = ps.ufid and ps_position_titles.dept_id = ps.dept_id
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
  puts "Create blank nodes"
  blank_node_people = create_blank_nodes(dbh)

  puts "Generate person rdf"
  ps_person_serializer = PsPersonSerializer.new
  people_rdf = RDF::Graph.new
  blank_node_people.each do |uri, ufid| 
    person_rdf = ps_person_serializer.create_rdf_for_person_in_ps(dbh, uri, ufid)
    people_rdf.insert(person_rdf)
  end

  puts "Generate type rdf"
  type_rdf = generate_type_rdf(dbh)

  puts "Generate position rdf"
  pos_rdf = generate_pos_rdf(dbh)

  RDF::Writer.open('new_people_rdf.nt') do |writer|
    type_rdf.each do |datum|
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
