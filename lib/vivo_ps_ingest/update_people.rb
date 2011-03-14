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

    def update_people
      updates = { :additions => RDF::Graph.new, :removals => RDF::Graph.new}
      @logger.info("Retrieving all people from vivo")
      vivo_rdf = retrieve_people_from_vivo
      ufids = vivo_rdf.query(:predicate => Person.predicates[:ufid])
      @logger.info("Finished retrieving all people from vivo")
      ufids.each_statement do |ufid_statement|
        uri = ufid_statement.subject
        ufid = ufid_statement.object.value
        @logger.info("Processing uri: #{uri} #{ufid}")
        dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
        difference = compare_person_in_ps_with_vivo(dbh, uri, ufid, vivo_rdf)
        if difference != {}
          first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
          first_name_addition = difference[:additions].query(:predicate => first_name_pred).first
          last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
          last_name_addition = difference[:additions].query(:predicate => last_name_pred).first
          # if first and last name isn't present we're not working with a 
          # valid record, so skip it
          if first_name_addition.nil? || last_name_addition.nil?
            @logger.info("Skipping uri: #{uri} #{ufid}")
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
      ps_person_serializer = PsPersonSerializer.new
      ps_rdf = ps_person_serializer.create_rdf_for_person_in_ps(dbh, uri, ufid)
      difference = difference_between_graphs(vivo_person_rdf, ps_rdf)
      return difference
    end
  end
end
