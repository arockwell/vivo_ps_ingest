require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

module VivoPsIngest
  describe UpdatePeople do
    before(:each) do
      @ufid = "81036590"
      @uri = "http://vivo.ufl.edu/individual/n1639"
      @predicates = {
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
        :gatorlink => RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink')
      }
    end

    it "should return difference between person_1 and person_2" do
      person_1 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_1.nt')
      person_2 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_2.nt')
      update_people = UpdatePeople.new
      differences = update_people.difference_between_graphs(person_1, person_2)
      differences[:removals].size.should == 1
      differences[:removals].first.object.value.should == "foo"
      differences[:additions].size.should == 1
      differences[:additions].first.object.value.should == "bar"
    end

    it "should show no differences between person_1 and person_1" do
      person_1 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_1.nt')
      update_people = UpdatePeople.new
      differences = update_people.difference_between_graphs(person_1, person_1)
      differences.should == {}
    end

    it "should show an extra property between person_1 and person_3" do
      person_1 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_1.nt')
      person_3 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_3.nt')
      update_people = UpdatePeople.new
      differences = update_people.difference_between_graphs(person_1, person_3)
      differences[:removals].size == 1
      differences[:removals].first.object.value.should == "foo"

      differences[:additions].size == 2
      name_statement = differences[:removals].first
      differences[:additions].has_statement?(name_statement).should == true
      phone_statement = RDF::Statement.new(name_statement.subject, RDF::URI.new("http://example.org/prop/phone"), "5555555555")
      differences[:additions].has_statement?(phone_statement).should == true
    end

    it "should retrieve a person record from vivo" do
      client = UpdatePeople.new
      results = client.retrieve_person_from_vivo(@ufid)
      results.size.should == 11
      check_predicate_value(results, @predicates[:first_name], "Alexander")
      check_predicate_value(results, @predicates[:last_name], "Rockwell")
      check_predicate_value(results, @predicates[:middle_name], "H")
      check_predicate_value(results, @predicates[:prefix_name], "Mr")
      check_predicate_value(results, @predicates[:work_phone], "3522732590")
      check_predicate_value(results, @predicates[:work_email], "alexhr@ufl.edu")
      results.query(:predicate => RDF::URI.new('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')).size.should == 5

      results_2 = results
    end

    it "should create rdf of their working title from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      update_people = UpdatePeople.new
      graph = update_people.create_work_title_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:hr_job_title], "Vivo Local Implementation Support")
    end

    it "should create rdf of their glid from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      update_people = UpdatePeople.new
      graph = update_people.create_glid_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:gatorlink], "alexhr")
    end

    it "should create rdf of their work_email from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
      
      update_people = UpdatePeople.new
      graph = update_people.create_work_email_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:work_email], "alexhr@ufl.edu")
    end

    it "should create name rdf from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      update_people = UpdatePeople.new
      graph = update_people.create_name_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:first_name], "Alexander")
      check_predicate_value(graph, @predicates[:last_name], "Rockwell")
      check_predicate_value(graph, @predicates[:middle_name], "H")
      check_predicate_value(graph, @predicates[:prefix_name], "Mr")
    end

    it "should create phone number rdf from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      update_people = UpdatePeople.new
      graph = update_people.create_phone_number_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:work_phone], "352.273.2590")
    end

    # tie everything together
    it "should create rdf for a person from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      update_people = UpdatePeople.new
      graph = update_people.create_rdf_for_person_in_ps(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:hr_job_title], "Vivo Local Implementation Support")
      check_predicate_value(graph, @predicates[:gatorlink], "alexhr")
      check_predicate_value(graph, @predicates[:work_email], "alexhr@ufl.edu")
      check_predicate_value(graph, @predicates[:first_name], "Alexander")
      check_predicate_value(graph, @predicates[:last_name], "Rockwell")
      check_predicate_value(graph, @predicates[:middle_name], "H")
      check_predicate_value(graph, @predicates[:prefix_name], "Mr")
      check_predicate_value(graph, @predicates[:work_phone], "352.273.2590")
    end

    def check_predicate_value(graph, predicate, expected_value)
      predicate.nil?.should == false
      graph.query(:predicate => predicate).first.object.value.should == expected_value
    end
    
  end
end
