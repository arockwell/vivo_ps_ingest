require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

module VivoPsIngest
  describe UpdatePeople do
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
  end
end
