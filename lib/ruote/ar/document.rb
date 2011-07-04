
require 'active_record'

# define document object.
class Ruote::Ar::Document < ActiveRecord::Base
  set_table_name :documents

  def self.before_fork
    ::ActiveRecord::Base.clear_all_connections!
  end

  def self.after_fork
    ::ActiveRecord::Base.establish_connection
  end
  
  def to_h
    ActiveSupport::JSON.decode doc
  end

  def to_wi
    Ruote::Workitem.from_json(doc)
  end
  
  def <=>(other)
    self.ide <=> other.ide
  end
end