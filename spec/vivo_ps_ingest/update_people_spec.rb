require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

module VivoPsIngest
  describe UpdatePeople do
    before(:each) do
      @ufid = "81036590"
      @uri = RDF::URI.new("http://vivo.ufl.edu/individual/n1639")
      @predicates = Person.predicates
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

    it "should show no difference between person_alex.ps.nt and person_alex.nt" do
      person_1 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_alex_ps.nt')
      person_3 = RDF::Graph.load(File.dirname(__FILE__) + '/../test/person_alex.nt')
      update_people = UpdatePeople.new
      differences = update_people.difference_between_graphs(person_1, person_3)
      differences.should == {}
    end

    # this does not test anything
    it "should update people" do
      client = UpdatePeople.new
      differences = client.update_people
      puts "REMOVALS"
      differences[:removals].each_statement {|x| puts x.inspect }
      puts "ADDITIONS"
      differences[:additions].each_statement {|x| puts x.inspect }
    end

    it "should serialize the graph to a file" do
      filename = File.dirname(__FILE__) + '/../test/temp.nt'
      graph = RDF::Graph.new
      graph.load(File.dirname(__FILE__) + '/../test/person_1.nt')

      update_people = UpdatePeople.new
      update_people.serialize_graph(graph, filename)

      graph_written = RDF::Graph.new
      graph_written.load(filename)

      val = graph.isomorphic_with? graph_written
      val.should == true
      File.delete(filename)
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
