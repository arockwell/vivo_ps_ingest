module VivoPsIngest
  class PsPersonSerializer
    def initialize
      @predicates = Person.predicates
    end

    def create_rdf_for_person_in_ps(dbh, uri, ufid)
      person = RDF::Graph.new
      person.insert(RDF::Statement(uri, @predicates[:ufid], ufid))
      person.insert(create_work_title_rdf(dbh, uri, ufid))
      person.insert(create_glid_rdf(dbh, uri, ufid))
      person.insert(create_work_email_rdf(dbh, uri, ufid))
      person.insert(create_name_rdf(dbh, uri, ufid))
      person.insert(create_phone_number_rdf(dbh, uri, ufid))

      return person
    end

    def create_work_title_rdf(dbh, uri, ufid)
      sql = "select work_title from psIngestDev.ps_employee_records where ufid = ?"

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      
      graph = RDF::Graph.new
      sth.fetch do |row|
        work_title = RDF::Literal.new(row[:work_title], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, @predicates[:hr_job_title], work_title)
      end
      
      return graph
    end
    
    def create_glid_rdf(dbh, uri, ufid)
      sql = "select glid from psIngestDev.ps_glid where ufid = ?"

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new 
      sth.fetch do |row|
        glid = RDF::Literal.new(row[:glid], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, @predicates[:gatorlink], glid)
      end
      return graph
    end

    def create_work_email_rdf(dbh, uri, ufid)
      sql = "select uf_email as work_email from psIngestDev.ps_employee_records ps where ufid = ?"

      sth = dbh.prepare(sql)
      sth.execute(ufid)

      graph = RDF::Graph.new
      sth.fetch do |row|
        graph << RDF::Statement(uri, @predicates[:work_email], row[:work_email])
      end
      return graph
    end

    def create_name_rdf(dbh, uri, ufid)
      sql = "select type_cd, name_text from psIngestDev.ps_names where ufid = ?"
      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new
      sth.fetch do |row|
        if row[:type_cd] == 35
          graph << RDF::Statement(uri, @predicates[:first_name], row[:name_text])
        elsif row[:type_cd] == 36
          graph << RDF::Statement(uri, @predicates[:last_name], row[:name_text])
        elsif row[:type_cd] == 37 && row[:type_cd] != ""
          graph << RDF::Statement(uri, @predicates[:middle_name], row[:name_text])
        elsif row[:type_cd] == 38
          graph << RDF::Statement(uri, @predicates[:prefix_name], row[:name_text])
        elsif row[:type_cd] == 39
          graph << RDF::Statement(uri, @predicates[:suffix_name], row[:name_text])
        elsif row[:type_cd] == 232
          label = RDF::Literal.new(row[:name_text], :language => 'en-US')
          graph << RDF::Statement(uri, @predicates[:label], label)
        end
      end
      return graph
    end

    def create_phone_number_rdf(dbh, uri, ufid)
      sql = <<-EOH
select ufid, type_cd, area_code, extension, phone_number 
from psIngestDev.ps_phone_numbers 
where ufid = ?
      EOH

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new
      sth.fetch do |row|
        # write out phone number in format NNN.NNN.NNNN xNNNN
        area_code = row[:area_code].nil? ? "" : row[:area_code].strip
        phone_number = row[:phone_number].nil? ? "" : row[:phone_number].strip
        extension = row[:extension].nil? ? "" : row[:extension].strip
        phone = "#{area_code}-#{phone_number.slice(0..2)}-#{phone_number.slice(3..6)}"
        phone = extension == "" ? phone : phone + " x" + extension

        if row[:type_cd] == "10"
          graph << RDF::Statement(uri, @predicates[:work_phone], phone)
        elsif row[:type_cd] == "11"
          graph << RDF::Statement(uri, @predicates[:work_fax], phone)
        end
      end
      return graph
    end
  end
end
