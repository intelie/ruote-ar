# -*- coding: utf-8 -*-
require 'ruote/part/storage_participant'
                   

class DateTime
  def usec
    self.to_time.usec
  end
end

class Module
  def alias_method_chain(target, feature)
    # Strip out punctuation on predicates or bang methods since
    # e.g. target?_without_feature is not a valid method name.
    aliased_target, punctuation = target.to_s.sub(/([?!=])$/, ''), $1
    yield(aliased_target, punctuation) if block_given?

    with_method, without_method = "#{aliased_target}_with_#{feature}#{punctuation}", "#{aliased_target}_without_#{feature}#{punctuation}"

    alias_method without_method, target
    alias_method target, with_method

    case
      when public_method_defined?(without_method)
        public target
      when protected_method_defined?(without_method)
        protected target
      when private_method_defined?(without_method)
        private target
    end
  end
end

#resolve sequel + meta_where conflict
module MetaWhere
  class Column
    def sql_literal(opts)
      "#{@column}"
    end
  end
end



#Deixando mais robusto, para multi thread worker

class << Thread
  alias orig_new new
  def new
    orig_new do
      begin
        yield
      ensure
        ActiveRecord::Base.connection_pool.release_connection if ActiveRecord::Base.connection_pool
      end
    end
  end
end


module ActiveRecord::ConnectionAdapters
  class Mysql2Adapter
    if self.respond_to? :execute
      alias_method :execute_without_retry, :execute
      def execute(*args)
        retries = 3
        begin
          execute_without_retry(*args)
        rescue ActiveRecord::StatementInvalid
          if $!.message =~ /server has gone away/i || $!.message =~ /lost connection to mysql/i
            Rails.logger.warn "Server timed out, retrying. #{retries} left."
            reconnect!
            retry if (retries -= 1) > 0
          end
          raise
        end
      end
    end
  end
end



#
# Simply opening the ruote workitem class to add some things specific
# to arts (and ActiveModel).
#
class Ruote::Workitem
  def task
    params['task']
  end

  def participant_name=(name)
    @h['participant_name'] = name
  end
end
  
                          
