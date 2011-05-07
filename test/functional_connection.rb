#
# testing ruote-redis
#
# Thu Apr  1 21:35:07 JST 2010
#

require 'yajl' rescue require 'json'
require 'rufus-json'
Rufus::Json.detect_backend
require 'active_support'
require 'ruote-ar'

class RrLogger
  def method_missing (m, *args)
    super if args.length != 1
    puts ". #{Time.now.to_f} #{Thread.current.object_id} #{args.first}"
  end
end



def new_storage (opts)
  # Database credentials
  ::Ruote::ActiveRecord::Storage.new(
    :adapter => 'mysql',
    :database => 'itsm_test',
    :username => 'root',
    :host => 'localhost'
  )  
end

