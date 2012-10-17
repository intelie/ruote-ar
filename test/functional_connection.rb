#
# testing ruote-ar
#
# Thu Feb 10 11:14:56 JST 2011
#

require 'rufus-json/automatic'
require 'ruote-ar'

# ENV['RUOTE_STORAGE_DB'] = "mysql2"
# unless $sequel
# 
#   $sequel = case ENV['RUOTE_STORAGE_DB'] || 'postgres'
#     when 'pg', 'postgres'
#       Sequel.connect('postgres://localhost/ruote_test')
#     when 'my', 'mysql'
#       #Sequel.connect('mysql://root:root@localhost/ruote_test')
#       Sequel.connect('mysql://root@localhost/ruote_test')
#     when 'mysql2'
#       Sequel.connect('mysql2://erp_user_d:adiFa81f83nDk@localhost/erpdb_d')
#       # Sequel.connect('mysql2://root@localhost/ruote_test')
#     when /:/
#       Sequel.connect(ENV['RUOTE_STORAGE_DB'])
#     else
#       raise ArgumentError.new("unknown DB: #{ENV['RUOTE_STORAGE_DB'].inspect}")
#   end
# 
#   require 'logger'
# 
#   logger = case ENV['RUOTE_STORAGE_DEBUG']
#     when 'log'
#       FileUtils.rm('debug.log') rescue nil
#       Logger.new('debug.log')
#     when 'stdout'
#       Logger.new($stdout)
#     else
#       nil
#   end
# 
#   if logger
#     logger.level = Logger::DEBUG
#     $sequel.loggers << logger
#   end
# 
#   Ruote::Sequel.create_table($sequel, true)
#     # true forces re_create of 'documents' table
# end


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