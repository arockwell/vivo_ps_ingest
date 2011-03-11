#!/usr/bin/ruby

require 'conf.rb'
require 'rdf/isomorphic'
require 'lib/vivo_ps_ingest/person.rb'
require 'lib/vivo_ps_ingest/ps_person_serializer.rb'
require 'lib/vivo_ps_ingest/update_people.rb'


update_people = VivoPsIngest::UpdatePeople.new
differences = update_people.update_people

removal_file = 'removals.nt'
addition_file = 'additions.nt'
update_people.serialize_graph(differences[:removals], removal_file)
update_people.serialize_graph(differences[:additions], addition_file)
