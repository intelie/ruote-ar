#
# testing ruote-ar
#
# Thu Feb 10 11:14:56 JST 2011
#

require 'rufus-json/automatic'
require 'ruote-ar'



# eg.
# opts = {
#  :adapter => 'mysql2',
#  :database => 'db',
#  :username => 'user', 
#  :password => 'passwd',                     
#  :pool => 10, #needed for eft_18
#  :host => 'localhost'
# }
def new_storage(opts)
  # Database credentials
  ::ActiveRecord::Base.establish_connection(opts)
  
  ::Ruote::ActiveRecord::Storage.new(opts)  
end
