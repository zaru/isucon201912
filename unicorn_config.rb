worker_processes 2
preload_app true
pid './unicorn.pid'
listen '/tmp/unicorn.sock'
listen 8080
