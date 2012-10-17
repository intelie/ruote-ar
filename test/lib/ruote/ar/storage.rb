# -*- coding: utf-8 -*-                
require 'rubygems'
require 'active_support/core_ext/module/aliasing'
require 'active_record' unless defined?(ActiveRecord)

# require 'meta_search' unless defined?(MetaSearch)

require 'rufus/json'
require 'ruote/storage/base'
Rufus::Json.backend = :active_support

#resolve sequel + meta_where conflict
# module MetaWhere
#   class Column
#     def sql_literal(opts)
#       "#{@column}"
#     end
#   end
# end

unless DateTime.instance_methods.include?(:usec)
  class DateTime
    def usec
      0
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

# module ActiveRecord::ConnectionAdapters
#   class Mysql2Adapter
#       alias_method :execute_without_retry, :execute
#       def execute(*args)
#         retries = 3
#         begin
#           execute_without_retry(*args)
#         rescue ActiveRecord::StatementInvalid
#           if $!.message =~ /server has gone away/i || $!.message =~ /lost connection to mysql/i ||
#               $!.message =~ /is still waiting for a result/i
#             
#             Rails.logger.warn "Server timed out, retrying. #{retries} left."
#             reconnect!
#             retry if (retries -= 1) > 0
#           end
#           raise
#         end
#     end
#   end
# end



module Ruote
  module ActiveRecord
    
    class RuoteDoc < ::ActiveRecord::Base
      self.table_name = 'ruote_docs'
      
      def self.before_fork
        ::ActiveRecord::Base.clear_all_connections!
      end

      def self.after_fork
        ::ActiveRecord::Base.establish_connection
      end
      
      def to_h
        return nil if doc.nil?
        doc_read = doc.respond_to?(:read) ? doc.read : doc
        Rufus::Json.decode(doc_read)
      end

      def to_wi
        Ruote::Workitem.from_json(doc)
      end
      
      def <=>(other)
        self.ide <=> other.ide
      end
    end
    
    class Storage
      include Ruote::StorageBase

      def initialize(options = {})
        
        #options['ar_table_name'] || 
        @table = ('ruote_docs').to_sym

        replace_engine_configuration(options)
      end      
      
      
      def put_msg(action, options)

        # put_msg is a unique action, no need for all the complexity of put

        do_insert(prepare_msg_doc(action, options), 1)

        nil
      end

      # Used to reserve 'msgs' and 'schedules'. Simply deletes the document,
      # return true if the delete was successful (ie if the reservation is
      # valid).
      #
      def reserve(doc)
        RuoteDoc.where(
          :typ => doc['type'], :ide => doc['_id'], :rev => 1
        ).delete_all > 0
      end
  
      def put_schedule(flavour, owner_fei, s, msg)

        # put_schedule is a unique action, no need for all the complexity of put

        doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

        return nil unless doc

        do_insert(doc, 1)

        doc['_id']
      end

      def put(doc, opts={})


        if doc['_rev']

          d = get(doc['type'], doc['_id'])

          return true unless d
          return d if d['_rev'] != doc['_rev']
            # failures
        end

        nrev = doc['_rev'].to_i + 1

        begin

          do_insert(doc, nrev, opts[:update_rev])

        rescue Exception => de
          puts "Error putting: #{de.message}: #{doc.inspect}"
          return (get(doc['type'], doc['_id']) || true)
            # failure
        end

        RuoteDoc.where("typ = ? and ide = ? and rev < ?", 
          doc['type'], doc['_id'], nrev
        ).delete_all

        nil
          # success
      end

      def get(type, key)

        d = do_get(type, key)

        d ? d.to_h : nil
      end

      def delete(doc)
        raise true if doc.nil?
        # failure
        raise ArgumentError.new('no _rev for doc') unless doc['_rev']

        count = do_delete(doc)

        return (get(doc['type'], doc['_id']) || true) if count < 1
        # failure

        nil
        # success
      end

      def get_many(type, key=nil, opts={})

        ds = RuoteDoc.where(:typ => type)

        keys = key ? Array(key) : nil
        ds = ds.where(:wfid => keys) if keys && keys.first.is_a?(String)

        return ds.all.size if opts[:count]
        
        if opts[:descending].is_a?(Array) && opts[:descending].first.class != String
          opts[:descending] = opts[:descending].collect {|s| s.inspect.gsub(':','').gsub('.', ' ')}
        end
        
        ds = ds.order(
                      *(opts[:descending] ? [ 'ide desc', 'rev desc' ] : [ 'ide asc', 'rev asc' ])
                      )

        ds = ds.limit(opts[:limit]).offset(opts[:skip]||opts[:offset])

        docs = ds.all
        docs = select_last_revs(docs)
        docs = docs.collect { |d| Rufus::Json.decode(d[:doc]) }

        if keys && keys.first.is_a?(Regexp) 
          docs.select { |doc| keys.find { |key| key.match(doc['_id']) } } 
        else
          docs
        end

        # (pass on the dataset.filter(:wfid => /regexp/) for now
        # since we have potentially multiple keys)
      end

      # Returns all the ids of the documents of a given type.
      #
      def ids(type)
        RuoteDoc.where(:typ => type).select(:ide).collect { |d| d[:ide] }.uniq.sort
      end

      # Nukes all the documents in this storage.
      #
      def purge!
        RuoteDoc.delete_all
      end

      # # Returns a string representation the current content of the storage for
      # # a given type.
      # #
      # def dump(type)
      # 
      #   "=== #{type} ===\n" +
      #     get_many(type).map { |h| "  #{h['_id']} => #{h.inspect}" }.join("\n")
      # end

      # Calls #disconnect on the db. According to Sequel's doc, it closes
      # all the idle connections in the pool (not the active ones).
      #
      def shutdown
        #Ruote::ActiveRecord::RuoteDoc.before_fork
      end

      # Grrr... I should sort the mess between close and shutdown...
      # Tests vs production :-(
      #
      def close
        #Ruote::ActiveRecord::RuoteDoc.before_fork
      end

      # Mainly used by ruote's test/unit/ut_17_storage.rb
      #
      def add_type(type)

        # does nothing, types are differentiated by the 'typ' column
      end

      # Nukes a db type and reputs it (losing all the documents that were in it).
      #
      def purge_type!(type)
        # puts "*** before purge type #{type},RuoteDoc.count:#{RuoteDoc.count}"
        RuoteDoc.delete_all(:typ => type)
        # puts "*** after purge type #{type},RuoteDoc.count:#{RuoteDoc.count}"
      end

      # A provision made for workitems, allow to query them directly by
      # participant name.
      #
      def by_participant(type, participant_name, opts={})

        raise NotImplementedError if type != 'workitems'

        docs = RuoteDoc.where('typ = ? and participant_name = ?', type, participant_name)
        docs = docs.order('ide asc, rev desc').limit(opts[:limit]).offset(opts[:offset] || opts[:skip])
        
        return docs.size if opts[:count]
        
        select_last_revs(docs).collect(&:to_wi)
      end

      # Querying workitems by field (warning, goes deep into the JSON structure)
      #
      def by_field(type, field, value, opts={})

        raise NotImplementedError if type != 'workitems'

        lk = [ '%"', field, '":' ]
        lk.push(Rufus::Json.encode(value)) if value
        lk.push('%')

        docs = RuoteDoc.where("typ = ? and doc like ?", type, lk.join)
        docs = docs.limit(opts[:limit]).offset(opts[:skip] || opts[:offset])

        return docs.size if opts[:count]
        select_last_revs(docs).collect(&:to_wi)
        
      end

      def query_workitems(criteria)

        ds = RuoteDoc.where(:typ => 'workitems')

        count = criteria.delete('count')
        limit = criteria.delete('limit')
        offset = criteria.delete('offset') || criteria.delete('skip')

        ds = ds.limit(limit).offset(offset)

        wfid =
          criteria.delete('wfid')
        pname =
          criteria.delete('participant_name') || criteria.delete('participant')

        ds = ds.where("ide like ?", "%!#{wfid}") if wfid
        ds = ds.where(:participant_name => pname) if pname

        criteria.collect do |k, v|
          ds = ds.where("doc like ?", "%\"#{k}\":#{Rufus::Json.encode(v)}%")
        end

        return ds.size if count
        select_last_revs(ds).collect(&:to_wi)
       
      end

      
      
      protected

      def do_delete(doc)
        RuoteDoc.delete_all(
          :ide => doc['_id'], :typ => doc['type'], :rev => doc['_rev'].to_i
        )
      end

      def do_insert(doc, rev, update_rev=false)
        
        
        # doc.merge!({'_rev' => rev, 'put_at' => Ruote.now_to_utc_s}) if update_rev
        doc = doc.send(
          update_rev ? :merge! : :merge,
          {'_rev' => rev, 'put_at' => Ruote.now_to_utc_s}
        )

        RuoteDoc.create!(
          :ide              => (doc['_id'] || ''),
          :rev              => (rev || ''),
          :typ              => (doc['type'] || ''),
          :doc              => (Rufus::Json.encode(doc) || ''),
          :wfid             => (extract_wfid(doc) || ''),
          :participant_name => (doc['participant_name'] || '')
        )
        
        # RuoteDoc.create!(
        #   :ide              => :$ide,
        #   :rev              => :$rev,
        #   :typ              => :$typ,
        #   :doc              => :$doc,
        #   :wfid             => :$wfid,
        #   :participant_name => :$participant_name
        # )
        
      end

      def extract_wfid(doc)

        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end

      def do_get(type, key)
        RuoteDoc.where(:typ => type, :ide => key).order('rev desc').first
      end

      # Don't put configuration if it's already in
      #
      # (avoid storages from trashing configuration...)
      #
      # def put_configuration
      # 
      #   return if get('configurations', 'engine')
      # 
      #   conf = { '_id' => 'engine', 'type' => 'configurations' }.merge(@options)
      #   put(conf)
      # end

      # Weed out older docs (same ide, smaller rev).
      #
      # This could all have been done via SQL, but those inconsistencies
      # are rare, the cost of the pumped SQL is not constant :-(
      #
      def select_last_revs(docs)
        docs.each_with_object([]) { |doc,a|
          a << doc if a.last.nil? || doc[:ide] != a.last[:ide]
        }
      end
      
    end
    
  end
end
