require 'logger'

module VivoPsIngest
  class UpdatePeople
    def initialize
      @logger = Logger.new('update_people.log')
    end

    # returns the difference between two graphs as a hash
    # :removals contains the graph to remove
    # :additions contains the graph to add
    #
    # if there is no changes returns {}
    def difference_between_graphs(graph_a, graph_b)
      difference = {}
      if graph_a.isomorphic_with? graph_b
        return difference
      else
        difference[:removals] = graph_a
        difference[:additions] = graph_b
      end
      return difference
    end

    # returns the graph people containing only their hr properties
    def retrieve_people_from_vivo
      sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
PREFIX bibo: <http://purl.org/ontology/bibo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>
PREFIX core: <http://vivoweb.org/ontology/core#>
PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#>

construct {
  ?person ufVivo:ufid ?ufid .
  ?person foaf:firstName ?first_name .
  ?person foaf:lastName ?last_name .
  ?person core:middleName ?middle_name .
  ?person bibo:prefixName ?prefix_name .
  ?person bibo:suffixName ?suffix_name .

  ?person core:workPhone ?work_phone .
  ?person core:workFax ?work_fax .

  ?person core:workEmail ?work_email .

  ?person rdfs:label ?label .
  
  ?person ufVivo:gatorlink ?glid . 
   
  ?person vitro:moniker ?moniker
}
where
{
  ?person ufVivo:ufid ?raw_ufid
  let (?ufid := str(?raw_ufid))

  ?person rdf:type foaf:Person .

  optional { ?person foaf:firstName ?first_name } 
  optional { ?person foaf:lastName ?last_name } 
  optional { ?person core:middleName ?middle_name }
  optional { ?person bibo:prefixName ?prefix_name }
  optional { ?person bibo:suffixName ?suffix_name }

  optional { ?person core:workPhone ?work_phone }
  optional { ?person core:workFax ?work_fax }

  optional { ?person core:workEmail ?work_email }

  optional { ?person rdfs:label ?label }
  
  optional { ?person ufVivo:gatorlink ?glid }
  
  optional { ?person vitro:moniker ?moniker }

  optional { ?person ufVivo:harvestedBy ?harvester}
  filter(str(?harvester) != "DSR-Harvester")

  ?person rdf:type ?type
}
      EOH

      hostname = ENV['vivo_hostname'] 
      username = ENV['vivo_username']
      password = ENV['vivo_password']
      sparql_client = VivoWebApi::Client.new(hostname)
      sparql_client.authenticate(username, password)
      results = sparql_client.execute_sparql_construct(username, password, sparql, "TTL")
      
      return results
    end

    # @TODO remove artificial limit to speed up tests
    def find_all_ufids_in_vivo
      sparql = <<-EOH
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>


select distinct ?uri ?ufid                                    
where                                                                     
{
  ?uri rdf:type foaf:Person .
  ?uri ufVivo:ufid ?raw_ufid .
  let (?ufid := str(?raw_ufid)) .
  optional { ?uri ufVivo:harvestedBy ?harvester}
  filter(str(?harvester) != "DSR-Harvester")
}
      EOH
      
      sparql_client = VivoWebApi::Client.new(ENV['vivo_hostname'])
      results = sparql_client.execute_sparql_select(ENV['vivo_username'], ENV['vivo_password'], sparql)

      ufid_uri_map = {}
      results.each do |result|
        ufid = result[:ufid].to_s
        ufid_uri_map[ufid] = result[:uri]
      end
        
      return ufid_uri_map

    end

    def update_people
      results = find_all_ufids_in_vivo
      updates = { :additions => RDF::Graph.new, :removals => RDF::Graph.new}
      @logger.info("Retrieving all people from vivo")
      vivo_rdf = retrieve_people_from_vivo
      @logger.info("Finished retrieving all people from vivo")
      results.keys.each do |ufid|
        @logger.info("Processing uri: #{results[ufid]} #{ufid}")
        dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
        difference = compare_person_in_ps_with_vivo(dbh, results[ufid], ufid, vivo_rdf)
        if difference != {}
          first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
          first_name_addition = difference[:additions].query(:predicate => first_name_pred).first
          last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
          last_name_addition = difference[:additions].query(:predicate => last_name_pred).first
          # if first and last name isn't present we're not working with a 
          # valid record, so skip it
          if first_name_addition.nil? || last_name_addition.nil?
            @logger.info("Skipping uri: #{results[ufid]} #{ufid}")
          else
            updates[:additions].insert(difference[:additions])
            updates[:removals].insert(difference[:removals])
          end
        end
      end
      return updates
    end

    def serialize_graph(graph, filename)
      RDF::Writer.open(filename) do |writer|
        graph.each_statement do |statement|
          writer << statement
        end
      end
    end

    def compare_person_in_ps_with_vivo(dbh, uri, ufid, vivo_rdf)
      ufid_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')
      vivo_person_uri = vivo_rdf.query(:predicate => ufid_pred, :object => RDF::Literal.new(ufid)).first.subject
      vivo_person_rdf = vivo_rdf.query(:subject => vivo_person_uri)
      vivo_person_rdf.each_statement {|x| puts x.inspect }
      ps_rdf = create_rdf_for_person_in_ps(dbh, uri, ufid)
      difference = difference_between_graphs(vivo_person_rdf, ps_rdf)
      return difference
    end

    def create_rdf_for_person_in_ps(dbh, uri, ufid)
      person = RDF::Graph.new
      person.insert(create_work_title_rdf(dbh, uri, ufid))
      person.insert(create_glid_rdf(dbh, uri, ufid))
      person.insert(create_work_email_rdf(dbh, uri, ufid))
      person.insert(create_name_rdf(dbh, uri, ufid))
      person.insert(create_phone_number_rdf(dbh, uri, ufid))

      return person
    end

    def create_work_title_rdf(dbh, uri, ufid)
      sql = "select work_title from psIngestDev.ps_employee_records where ufid = ?"

      hr_job_title_pred = RDF::URI.new('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#moniker')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      
      graph = RDF::Graph.new
      sth.fetch do |row|
        work_title = RDF::Literal.new(row[:work_title], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, hr_job_title_pred, work_title)
      end
      
      return graph
    end
    
    def create_glid_rdf(dbh, uri, ufid)
      sql = "select glid from psIngestDev.ps_glid where ufid = ?"

      gatorlink_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new 
      sth.fetch do |row|
        glid = RDF::Literal.new(row[:glid], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, gatorlink_pred, glid)
      end
      return graph
    end

    def create_work_email_rdf(dbh, uri, ufid)
      sql = "select uf_email as work_email from psIngestDev.ps_employee_records ps where ufid = ?"

      work_email_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workEmail')

      sth = dbh.prepare(sql)
      sth.execute(ufid)

      graph = RDF::Graph.new
      sth.fetch do |row|
        graph << RDF::Statement(uri, work_email_pred, row[:work_email])
      end
      return graph
    end

    def create_name_rdf(dbh, uri, ufid)
      sql = "select type_cd, name_text from psIngestDev.ps_names where ufid = ?"
      first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
      last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
      middle_name_pred = RDF::URI.new('http://vivoweb.org/ontology/core#middleName') 
      label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')
      prefix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/prefixName')
      suffix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/suffixName')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new
      sth.fetch do |row|
        if row[:type_cd] == 35
          graph << RDF::Statement(uri, first_name_pred, row[:name_text])
        elsif row[:type_cd] == 36
          graph << RDF::Statement(uri, last_name_pred, row[:name_text])
        elsif row[:type_cd] == 37 && row[:type_cd] != ""
          graph << RDF::Statement(uri, middle_name_pred, row[:name_text])
        elsif row[:type_cd] == 38
          graph << RDF::Statement(uri, prefix_name_pred, row[:name_text])
        elsif row[:type_cd] == 39
          graph << RDF::Statement(uri, suffix_name_pred, row[:name_text])
        elsif row[:type_cd] == 232
          label = RDF::Literal.new(row[:name_text], :language => 'en-US')
          graph << RDF::Statement(uri, label_pred, label)
        end
      end
      return graph
    end

    def create_phone_number_rdf(dbh, uri, ufid)
      sql = <<-EOH
select ufid, type_cd, area_code, extension, phone_number 
from psIngestDev.ps_phone_numbers 
where ufid = ?
      EOH

      work_phone_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workPhone')
      work_fax_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workFax')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new
      sth.fetch do |row|
        # write out phone number in format NNN.NNN.NNNN xNNNN
        area_code = row[:area_code].nil? ? "" : row[:area_code].strip
        phone_number = row[:phone_number].nil? ? "" : row[:phone_number].strip
        extension = row[:extension].nil? ? "" : row[:extension].strip
        phone = "#{area_code}-#{phone_number.slice(0..2)}-#{phone_number.slice(3..6)}"
        phone = extension == "" ? phone : phone + " x" + extension

        if row[:type_cd] == "10"
          graph << RDF::Statement(uri, work_phone_pred, phone)
        elsif row[:type_cd] == "11"
          graph << RDF::Statement(uri, work_fax_pred, phone)
        end
      end
      return graph
    end

  end
end
