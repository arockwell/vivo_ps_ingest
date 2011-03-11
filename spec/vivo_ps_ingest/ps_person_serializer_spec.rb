require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

module VivoPsIngest
  describe PsPersonSerializer do
    before(:each) do
      @ufid = "81036590"
      @uri = RDF::URI.new("http://vivo.ufl.edu/individual/n1639")
      @predicates = VivoPsIngest::Person.predicates
    end

    it "should create rdf of their working title from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_work_title_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:hr_job_title], "Vivo Local Implementation Support")
    end

    it "should create rdf of their glid from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_glid_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:gatorlink], "alexhr")
    end

    it "should create rdf of their work_email from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])
      
      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_work_email_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:work_email], "alexhr@ufl.edu")
    end

    it "should create name rdf from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_name_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:first_name], "Alexander")
      check_predicate_value(graph, @predicates[:last_name], "Rockwell")
      check_predicate_value(graph, @predicates[:middle_name], "H")
      check_predicate_value(graph, @predicates[:prefix_name], "Mr")
    end

    it "should create phone number rdf from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_phone_number_rdf(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:work_phone], "352-273-2590")
    end

    # tie everything together
    it "should create rdf for a person from peoplesoft" do
      dbh = DBI.connect(ENV['mysql_connection'], ENV['mysql_username'], ENV['mysql_password'])

      ps_person_serializer = PsPersonSerializer.new
      graph = ps_person_serializer.create_rdf_for_person_in_ps(dbh, @uri, @ufid)
      check_predicate_value(graph, @predicates[:hr_job_title], "Vivo Local Implementation Support")
      check_predicate_value(graph, @predicates[:gatorlink], "alexhr")
      check_predicate_value(graph, @predicates[:work_email], "alexhr@ufl.edu")
      check_predicate_value(graph, @predicates[:first_name], "Alexander")
      check_predicate_value(graph, @predicates[:last_name], "Rockwell")
      check_predicate_value(graph, @predicates[:middle_name], "H")
      check_predicate_value(graph, @predicates[:prefix_name], "Mr")
      check_predicate_value(graph, @predicates[:work_phone], "352-273-2590")
    end

    def check_predicate_value(graph, predicate, expected_value)
      predicate.nil?.should == false
      graph.query(:predicate => predicate).first.object.value.should == expected_value
    end

    def find_subgraph(graph, predicate)
      predicate.nil?.should == false
      graph.query(:predicate => predicate)
    end
  end
end
