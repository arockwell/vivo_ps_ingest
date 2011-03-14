module VivoPsIngest
  class Person
    # @TODO: Find a better way to represent this.
    def self.predicates
      predicates = {
        :first_name => RDF::URI.new('http://xmlns.com/foaf/0.1/firstName'),
        :last_name => RDF::URI.new('http://xmlns.com/foaf/0.1/lastName'),
        :middle_name => RDF::URI.new('http://vivoweb.org/ontology/core#middleName') ,
        :label => RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label'),
        :prefix_name => RDF::URI.new('http://purl.org/ontology/bibo/prefixName'),
        :suffix_name => RDF::URI.new('http://purl.org/ontology/bibo/suffixName'),
        :work_phone => RDF::URI.new('http://vivoweb.org/ontology/core#workPhone'),
        :work_fax => RDF::URI.new('http://vivoweb.org/ontology/core#workFax'),
        :work_email => RDF::URI.new('http://vivoweb.org/ontology/core#workEmail'),
        :hr_job_title => RDF::URI.new('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#moniker'),
        :gatorlink => RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink'),
        :ufid => RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/ufid')
      }
    end
  end
end
