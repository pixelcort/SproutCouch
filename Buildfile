config :all, :required => [:sproutcore, :SproutCouchFramework]

proxy '/scd', :to => 'localhost:5984'
