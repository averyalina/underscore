require('sequel')

DBEU = Sequel.connect(:adapter => 'mysql2', :user => 'sequelpro', :host => '127.0.0.1', :database => 'joliebox',:password=>'2hLGlwfpdaJO', :port => 44445)
DB = Sequel.connect(:adapter => 'mysql2', :user => 'benjamin', :host => '127.0.0.1', :database => 'magento_birchbox',:password=>'gxU9Jn8Vhv39hJMx', :port => 44444)
