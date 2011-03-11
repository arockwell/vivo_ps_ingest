module VivoPsIngest
  class PsPersonSerializer
    def create_rdf_for_person_in_ps(dbh, uri, ufid)
      person = RDF::Graph.new
      person.insert(create_work_title_rdf(dbh, uri, ufid))
      person.insert(create_glid_rdf(dbh, uri, ufid))
      person.insert(create_work_email_rdf(dbh, uri, ufid))
      person.insert(create_name_rdf(dbh, uri, ufid))
      person.insert(create_phone_number_rdf(dbh, uri, ufid))

      return person
    end

    def create_work_title_rdf(dbh, uri, ufid)
      sql = "select work_title from psIngestDev.ps_employee_records where ufid = ?"

      hr_job_title_pred = RDF::URI.new('http://vitro.mannlib.cornell.edu/ns/vitro/0.7#moniker')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      
      graph = RDF::Graph.new
      sth.fetch do |row|
        work_title = RDF::Literal.new(row[:work_title], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, hr_job_title_pred, work_title)
      end
      
      return graph
    end
    
    def create_glid_rdf(dbh, uri, ufid)
      sql = "select glid from psIngestDev.ps_glid where ufid = ?"

      gatorlink_pred = RDF::URI.new('http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new 
      sth.fetch do |row|
        glid = RDF::Literal.new(row[:glid], :datatype => RDF::XSD.string)
        graph << RDF::Statement(uri, gatorlink_pred, glid)
      end
      return graph
    end

    def create_work_email_rdf(dbh, uri, ufid)
      sql = "select uf_email as work_email from psIngestDev.ps_employee_records ps where ufid = ?"

      work_email_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workEmail')

      sth = dbh.prepare(sql)
      sth.execute(ufid)

      graph = RDF::Graph.new
      sth.fetch do |row|
        graph << RDF::Statement(uri, work_email_pred, row[:work_email])
      end
      return graph
    end

    def create_name_rdf(dbh, uri, ufid)
      sql = "select type_cd, name_text from psIngestDev.ps_names where ufid = ?"
      first_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/firstName')
      last_name_pred = RDF::URI.new('http://xmlns.com/foaf/0.1/lastName')
      middle_name_pred = RDF::URI.new('http://vivoweb.org/ontology/core#middleName') 
      label_pred = RDF::URI.new('http://www.w3.org/2000/01/rdf-schema#label')
      prefix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/prefixName')
      suffix_name_pred = RDF::URI.new('http://purl.org/ontology/bibo/suffixName')

      sth = dbh.prepare(sql)
      sth.execute(ufid)
      graph = RDF::Graph.new
      sth.fetch do |row|
        if row[:type_cd] == 35
          graph << RDF::Statement(uri, first_name_pred, row[:name_text])
        elsif row[:type_cd] == 36
          graph << RDF::Statement(uri, last_name_pred, row[:name_text])
        elsif row[:type_cd] == 37 && row[:type_cd] != ""
          graph << RDF::Statement(uri, middle_name_pred, row[:name_text])
        elsif row[:type_cd] == 38
          graph << RDF::Statement(uri, prefix_name_pred, row[:name_text])
        elsif row[:type_cd] == 39
          graph << RDF::Statement(uri, suffix_name_pred, row[:name_text])
        elsif row[:type_cd] == 232
          label = RDF::Literal.new(row[:name_text], :language => 'en-US')
          graph << RDF::Statement(uri, label_pred, label)
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

      work_phone_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workPhone')
      work_fax_pred = RDF::URI.new('http://vivoweb.org/ontology/core#workFax')

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
          graph << RDF::Statement(uri, work_phone_pred, phone)
        elsif row[:type_cd] == "11"
          graph << RDF::Statement(uri, work_fax_pred, phone)
        end
      end
      return graph
    end
  end
end
