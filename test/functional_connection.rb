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
end

def new_storage(opts)
  Ruote::ActiveRecord::Storage.new(opts)
end

# implement purge! and purge_type! just for test
module Ruote
  module ActiveRecord
    class Storage
      def purge_type!(type)
        dm = Arel::DeleteManager.new Arel::Table.engine
        dm.from table
        dm.where table[:typ].eq(type)
        connection.delete(dm)
      end

      def purge!
        dm = Arel::DeleteManager.new Arel::Table.engine
        dm.from table
        connection.delete(dm)
      end
    end
  end
end
