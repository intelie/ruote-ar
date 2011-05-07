Gem::Specification.new do |s|
  s.name              = 'ruote-ar'
  s.version           = '0.0.1'
  s.authors           = ["Pedro"]
  s.summary           = 'ruote storage'
  s.description       = "ruote storage"
  s.email             = ['pedro@intelie.com.br']
  s.files             = Dir.glob('{lib}/**/*') 
  s.require_paths     = ["lib"]

  s.add_runtime_dependency      'activesupport',  '~> 3.0'

  s.add_development_dependency  'rails',          '~> 3.0'
  s.add_development_dependency  'mysql'
end