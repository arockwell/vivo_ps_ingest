module VivoPsIngest
  class UpdatePeople
    # returns the difference between two  graphs
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
  end
end
