module VivoPsIngest
end

require 'rubygems'
gem 'dbi'
require 'dbi'
gem 'VivoWebApi'
require 'vivo_web_api'
require 'rdf/isomorphic'
require 'rdf/ntriples'

require 'vivo_ps_ingest/person'
require 'vivo_ps_ingest/update_people'
require 'vivo_ps_ingest/ps_person_serializer'

require '~/.passwords'
