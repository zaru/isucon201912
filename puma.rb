root = "#{Dir.getwd}"

bind "unix:///tmp/unicorn.sock"
pidfile "#{root}/unicorn.pid"
state_path "/tmp/state"
rackup "#{root}/config.ru"
threads 4, 16
activate_control_app
daemonize true
workers 2
preload_app!
