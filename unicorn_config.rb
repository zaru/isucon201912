worker_processes 2
preload_app true
pid './unicorn.pid'
backlog 4096
listen '/tmp/unicorn.sock'
listen 8080
