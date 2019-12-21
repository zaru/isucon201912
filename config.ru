require_relative './app.rb'
# これいれると逆に遅くなるな
#require 'dalli'
#require 'rack/cache'
#
#use Rack::Cache,
#    verbose:  true,
#    metastore:    "memcached://localhost:11211/meta",
#    entitystore:  "memcached://localhost:11211/body"

run Ishocon2::WebApp
