#!/bin/bash

echo "Dump ufids in vivo"
ruby dump_vivo_ufids.rb

echo "Dump orgs in vivo"
ruby dump_vivo_orgs.rb

echo "Dump faculty names in peoplesoft"
ruby dump_faculty_names.rb

echo "Dump glid in peoplesoft"
ruby dump_glid.rb

echo "Dump phone numbers in peoplesoft"
ruby dump_phone_numbers.rb

echo "Dump work emails in peoplesoft"
ruby dump_work_email.rb

echo "Dump position data in peoplesoft"
ruby dump_positions.rb

echo "Create new faculty rdf"
ruby create_new_faculty.rb

