require 'ruote-ar'

class RrLogger
  def method_missing (m, *args)
    super if args.length != 1
    puts ". #{Time.now.to_f} #{Thread.current.object_id} #{args.first}"
  end
end



def new_storage (opts)
  # Database credentials
  ::ActiveRecord::Base.establish_connection({:adapter => 'mysql2',
                                              :database => 'itsm_test',
                                              :username => 'root',                      
                                              :pool => 10, #needed for eft_18
                                              :host => 'localhost'})
  
  ::Ruote::ActiveRecord::Storage.new(opts)  
end

