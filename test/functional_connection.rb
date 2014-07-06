# testing ruote-ar

require 'rufus-json/automatic'
require 'ruote-ar'
require 'activerecord-jdbcmysql-adapter'

# Database credentials
unless ActiveRecord::Base.connected?
  ActiveRecord::Base.establish_connection(
    adapter: 'mysql2',
    host: 'localhost',
    database: 'ruote_ar_test',
    encoding: 'utf8',
    username: 'root',
    timeout: 100,
    pool: 4 # main, worker thread
  )
  # force connection
  ActiveRecord::Base.connection
end

def new_storage(opts)
  Ruote::ActiveRecord::Storage.new(opts)
end
