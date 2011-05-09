require 'rubygems'
require 'active_support/core_ext/module/aliasing.rb'
require 'active_record' unless defined?(ActiveRecord)
require 'mysql2'
require 'meta_where' unless defined?(MetaWhere)
require 'ruote/storage/base'
require 'ruote/ar/ruote_patch'
require 'ruote/ar/storage'