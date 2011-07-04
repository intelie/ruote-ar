# require 'ruote/ar/storage'

require 'active_support'

module Ruote
  module Ar
    extend ActiveSupport::Autoload
    autoload :Document
    autoload :Storage
  end
end
