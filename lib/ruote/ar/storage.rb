# encoding: utf-8

require 'active_record'
require 'ruote/storage/base'

Rufus::Json.backend = :active_support

module Ruote
  module ActiveRecord
    class Storage
      include Ruote::StorageBase

      def initialize(options = {})

        @table_name = options['table_name'] || ('documents').to_sym

        replace_engine_configuration(options)
      end


      def put_msg(action, options)

        # put_msg is a unique action, no need for all the complexity of put
        do_insert(prepare_msg_doc(action, options), 1)

        nil
      end

      # Used to reserve 'msgs' and 'schedules'. Simply update and
      # return true if the update was affected more than one line.
      #
      def reserve(doc)
        um = Arel::UpdateManager.new Arel::Table.engine
        um.table table
        um.where table[:typ].eq(doc['type'].to_s).and(table[:ide].eq(doc['_id'].to_s).and(table[:rev].eq(1).and(table[:worker].eq(nil))))
        um.set [
          [table[:worker], worker]
        ]
        connection.update(um.to_sql) > 0
      end

      # removing doc after success (or fail) success.
      # It's important to not leave any message.
      def done(doc)
        dm = Arel::DeleteManager.new Arel::Table.engine
        dm.from table
        dm.where table[:typ].eq(doc['type']).and(table[:ide].eq(doc['_id']).and(table[:rev].eq(1).and(table[:worker].eq(worker))))
        connection.delete(dm)
      end

      def put_schedule(flavour, owner_fei, s, msg)

        # put_schedule is a unique action, no need for all the complexity of put

        doc = prepare_schedule_doc(flavour, owner_fei, s, msg)

        return nil unless doc

        do_insert(doc, 1)

        doc['_id']
      end

      def put(doc, opts={})

        cache_clear(doc)

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
        
        dm = Arel::DeleteManager.new Arel::Table.engine
        dm.from table
        dm.where table[:typ].eq(doc['type']).and(table[:ide].eq(doc['_id']).and(table[:rev].lt(nrev)))
        connection.delete(dm)

        nil
          # success
      end

      def get(type, key)
        cache_get(type, key) || do_get(type, key)
      end

      def delete(doc)
        raise true if doc.nil?
       
        raise ArgumentError.new('no _rev for doc') unless doc['_rev']

        cache_clear(doc)
          # usually not necessary, adding it not to forget it later on        
      
        dm = Arel::DeleteManager.new Arel::Table.engine
        dm.from table
        dm.where table[:typ].eq(doc['type']).and(table[:ide].eq(doc['_id']).and(table[:rev].eq(doc['_rev'].to_i)))
        count = connection.delete(dm)

        return (get(doc['type'], doc['_id']) || true) if count < 1
        # failure

        nil
        # success
      end

      def get_many(type, key=nil, opts={})

        ### 

        cached = cache_get_many(type, key, opts)
        return cached if cached

        ds = table[:typ].eq(type)

        keys = key ? Array(key) : nil
        ds = ds.and(table[:wfid].in(keys)) if keys && keys.first.is_a?(String)
        
        ds = table.where(ds)

        return connection.select_value(ds.project(table[:wfid].count)) if opts[:count]

        if opts[:descending].is_a?(Array) && opts[:descending].first.class != String
          opts[:descending] = opts[:descending].collect {|s| s.inspect.gsub(':','').gsub('.', ' ')}
        end

        if opts[:descending]
          ds = ds.order(table[:ide].desc, table[:rev].desc)
        else 
          ds = ds.order(table[:ide].asc, table[:rev].asc)
        end

        ds = ds.take(opts[:limit]).skip(opts[:skip]||opts[:offset])

        docs = connection.select_all(ds.project('*'))
        docs = select_last_revs(docs)
        docs = docs.collect { |d| Rufus::Json.decode(d['doc']) }

        if keys && keys.first.is_a?(Regexp)
          docs.select { |doc| keys.find { |k| k.match(doc['_id']) } }
        else
          docs
        end

        # (pass on the dataset.filter(:wfid => /regexp/) for now
        # since we have potentially multiple keys)
      end

      # Returns all the ids of the documents of a given type.
      #
      def ids(type)
        connection.select_values(table.where(table[:typ].eq(type)).project('distinct ide').order(table[:ide]))
      end

      # Nukes all the documents in this storage.
      #
      def purge!
        # just for test
      end

      # Returns connection to pool
      def shutdown
        ::ActiveRecord::Base.clear_active_connections!
        ::ActiveRecord::Base.connection.close
      end

      # Grrr... I should sort the mess between close and shutdown...
      # Tests vs production :-(
      #
      def close
        shutdown
      end

      # Mainly used by ruote's test/unit/ut_17_storage.rb
      #
      def add_type(type)
        # does nothing, types are differentiated by the 'typ' column
      end

      # Nukes a db type and reputs it (losing all the documents that were in it).
      #
      def purge_type!(type)
        # just for test
      end

      # A provision made for workitems, allow to query them directly by
      # participant name.
      #
      def by_participant(type, participant_name, opts={})

        raise NotImplementedError if type != 'workitems'

        docs = table.where(table[:typ].eq(type).and(table[:participant_name].eq(participant_name)))

        return connection.select_value(docs.project('count(*)')) if opts[:count]
        
        docs = connection.select_all(docs.project('*').order(table[:ide].asc, table[:rev].desc).take(opts[:limit]).skip(opts[:offset] || opts[:skip]))

        select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d['doc']) }
      end

      # Querying workitems by field (warning, goes deep into the JSON structure)
      #
      def by_field(type, field, value, opts={})

        raise NotImplementedError if type != 'workitems'

        lk = [ '%"', field, '":' ]
        lk.push(Rufus::Json.encode(value)) if value
        lk.push('%')

        docs = table.where(table[:typ].eq(type).and(table[:doc].matches(lk.join)))

        return connection.select_value(docs.project('count(*)')) if opts[:count]
        
        docs = connection.select_all(docs.project('*').order(table[:ide].asc, table[:rev].desc).take(opts[:limit]).skip(opts[:offset] || opts[:skip]))
        select_last_revs(docs).collect { |d| Ruote::Workitem.from_json(d['doc']) }
      end

      def query_workitems(criteria)

        ds = table[:typ].eq('workitems')
        
        wfid = criteria.delete('wfid')
        ds = ds.and(table[:ide].matches("%!#{wfid}")) if wfid
        
        pname = criteria.delete('participant_name') || criteria.delete('participant')
        ds = ds.and(table[:participant_name].eq(pname)) if pname

        count = criteria.delete('count')
        limit = criteria.delete('limit')
        offset = criteria.delete('offset') || criteria.delete('skip')

        criteria.collect do |k, v|
          ds = ds.and(table[:doc].matches("%\"#{k}\":#{Rufus::Json.encode(v)}%"))
        end
        
        ds = table.where(ds).take(limit).skip(offset)

        return connection.select_one(ds.project(table[:wfid].count)).first if count
        
        select_last_revs(connection.select_all(ds.project('*'))).collect { |d| Ruote::Workitem.from_json(d['doc']) } 
      end
      
      def begin_step

        prepare_cache
      end 

      protected

      def decode_doc(doc)

        return nil if doc.nil?

        doc = doc['doc']

        Rufus::Json.decode(doc)
      end


      def do_insert(doc, rev, update_rev=false)

        doc = doc.send(
          update_rev ? :merge! : :merge,
          {'_rev' => rev, 'put_at' => Ruote.now_to_utc_s}
        )
        
        m = Arel::InsertManager.new(Arel::Table.engine)
        m.into table
        m.insert [  
          [table[:ide], (doc['_id'] || '')],
          [table[:rev], (rev || '')],
          [table[:typ], (doc['type'] || '')],
          [table[:doc], (Rufus::Json.encode(doc) || '')],
          [table[:wfid], (extract_wfid(doc) || '')],
          [table[:participant_name], (doc['participant_name'] || '')]]

          connection.insert(m)
      end
      

      def extract_wfid(doc)
        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end

      def do_get(type, key)
        decode_doc connection.select_one(table.project('*').
                                         where(table[:typ].eq(type).and(table[:ide].eq(key))).
                                         order(table[:rev].desc))
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
          a << doc if a.last.nil? || doc['ide'] != a.last['ide']
        }
      end

      #--
      # worker step cache
      #
      # in order to cut down the number of selects, do one select with
      # all the information the worker needs for one step of work
      #++


      CACHED_TYPES = %w[ msgs schedules configurations variables ]

      # One select to grab in all the info necessary for a worker step
      # (expressions excepted).
      #
      def prepare_cache

        CACHED_TYPES.each { |t| cache[t] = {} }

        connection.select_all(table.where(table[:typ].in(CACHED_TYPES)).
                              project(table[:ide], table[:typ], table[:doc]).
                              order(table[:ide].asc, table[:rev].desc)).each do |d|
          (cache[d['typ']] ||= {})[d['ide']] ||= decode_doc(d)
                              end

        cache['variables']['trackers'] ||=
          { '_id' => 'trackers', 'type' => 'variables', 'trackers' => {} }
      end

    # Ask the cache for a doc. Returns nil if it's not cached.
    #
    def cache_get(type, key)

      (cache[type] || {})[key]
    end

    # Ask the cache for a set of documents. Returns nil if it's not cached
    # or caching is not OK.
    #
    def cache_get_many(type, keys, options)

      if !options[:batch] && CACHED_TYPES.include?(type) && cache[type]
        cache[type].values
      else
        nil
      end
    end

    # Removes a document from the cache.
    #
    def cache_clear(doc)

      (cache[doc['type']] || {}).delete(doc['_id'])
    end

    # Returns the cache for the given thread. Returns {} if there is no
    # cache available.
    #
    def cache

      worker = Thread.current['ruote_worker']

      return {} unless worker

      (Thread.current["cache_#{worker.name}"] ||= {})
    end


      # def do_delete(doc)
      #   Document.delete_all(
      #     :ide => doc['_id'], :typ => doc['type'], :rev => doc['_rev'].to_i
      #   )
      # end

      private
      def table
        @table ||= ::Arel::Table.new @table_name
      end

      def connection
        ::ActiveRecord::Base.connection
      end

      def worker
        worker = Thread.current['ruote_worker']
        if worker
          worker.name
        end || 'worker'
      end
    end
  end
end
