require "rack/cache"
require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require 'socket'
require "redis"

module Ishocon2
  class AuthenticationError < StandardError; end
  class PermissionDenied < StandardError; end
end

class Ishocon2::WebApp < Sinatra::Base
  session_secret = ENV['ISHOCON2_SESSION_SECRET'] || 'showwin_happy'
  use Rack::Session::Cookie, key: 'rack.session', secret: session_secret
  set :erb, escape_html: true
  set :public_folder, File.expand_path('../public', __FILE__)
  set :protection, true



  helpers do
    def redis
      @redis_con ||= Redis.new
    end
    def config
      @config ||= {
        db: {
          host: ENV['ISHOCON2_DB_HOST'] || 'localhost',
          port: ENV['ISHOCON2_DB_PORT'] && ENV['ISHOCON2_DB_PORT'].to_i,
          username: ENV['ISHOCON2_DB_USER'] || 'ishocon',
          password: ENV['ISHOCON2_DB_PASSWORD'] || 'ishocon',
          database: ENV['ISHOCON2_DB_NAME'] || 'ishocon2'
        }
      }
    end

    def db
      return Thread.current[:ishocon2_db] if Thread.current[:ishocon2_db]
      client = Mysql2::Client.new(
        host: config[:db][:host],
        port: config[:db][:port],
        username: config[:db][:username],
        password: config[:db][:password],
        database: config[:db][:database],
        reconnect: true
      )
      client.query_options.merge!(symbolize_keys: true)
      Thread.current[:ishocon2_db] = client
      client
    end

    def election_results
      query = <<SQL
SELECT c.id, c.name, c.political_party, c.sex, v.count
FROM candidates AS c
LEFT OUTER JOIN
  (SELECT candidate_id, COUNT(candidate_id) AS count
  FROM votes
  GROUP BY candidate_id) AS v
ON c.id = v.candidate_id
ORDER BY v.count DESC
SQL
      db.xquery(query)
    end

    def voice_of_supporter(candidate_ids)
      keys = []
      candidate_ids.each do |c_id|
        keys << "c_key_#{c_id}"
      end
      redis.zunionstore("c_ids_#{candidate_ids.join('-')}", keys)
      redis.zrevrange "c_ids_#{candidate_ids.join('-')}", 0, 9
    end

    def db_initialize
      clear_nginx_cache
      redis.flushdb
    end

    def memcache_flush
      server  = '127.0.0.1'
      port    = 11211
      command = "flush_all\r\n"

      socket = TCPSocket.new(server, port)
      socket.write(command)
      result = socket.recv(2)
      socket.close
    end

    def clear_nginx_cache
      system('rm -rf /var/cache/nginx/*')
    end
  end

  get '/' do
    top10_ids = fetch_top_c_id_top10

    query = <<SQL
SELECT id, name, political_party, sex
FROM candidates
WHERE id IN (?)
SQL
    c_users = db.xquery(query, top10_ids)
    users = []
    c_users.each do |data|
      users << data
    end
    users.each_with_index do |val, index|
      users[index][:count] = fetch_count(val[:id])
    end
    users.sort_by! { |a| a[:count] }.reverse!

    candidates = users + [fetch_top_c_id_last]

    parties_set = db.query('SELECT political_party FROM candidates GROUP BY political_party')
    parties = {}
    parties_set.each { |a| parties[a[:political_party]] = 0 }

    parties_rank = fetch_top_parties
    parties_rank.each do |r|
      parties[r] += get_parties(r).to_i || 0
    end

    sex_ratio = { '男': 0, '女': 0 }
    sex_rank = fetch_top_sex
    sex_rank.each do |r|
      sex_ratio[r.to_sym] += get_sex(r).to_i || 0
    end

    erb :index, locals: { candidates: candidates,
                          parties: parties,
                          sex_ratio: sex_ratio }
  end

  get '/candidates/:id' do
    cache_control :public, :max_age => 86400
    candidate = db.xquery('SELECT political_party, name, sex FROM candidates WHERE id = ?', params[:id]).first
    return redirect '/' if candidate.nil?

    votes = fetch_count params[:id]
    keywords = voice_of_supporter([params[:id]])
    erb :candidate, locals: { candidate: candidate,
                              votes: votes,
                              keywords: keywords }
  end

  get '/political_parties/:name' do
    cache_control :public, :max_age => 86400

    votes = get_parties(params[:name]).to_i || 0
    candidates = db.xquery('SELECT id, name FROM candidates WHERE political_party = ?', params[:name])
    candidate_ids = candidates.map { |c| c[:id] }
    keywords = voice_of_supporter(candidate_ids)
    erb :political_party, locals: { political_party: params[:name],
                                    votes: votes,
                                    candidates: candidates,
                                    keywords: keywords }
  end

  get '/vote' do
    cache_control :public, :max_age => 86400
    candidates = db.query('SELECT name FROM candidates')
    erb :vote, locals: { candidates: candidates, message: '' }
  end

  post '/vote' do
    user = db.xquery('SELECT * FROM users WHERE name = ? AND address = ? AND mynumber = ?',
                     params[:name],
                     params[:address],
                     params[:mynumber]).first
    #TODO: ここ id で fetch できないかな？
    candidate = db.xquery('SELECT * FROM candidates WHERE name = ? limit 1', params[:candidate]).first

    voted_count = user.nil? ? 0 : fetch_count_user(user[:id])
    voted_count = 0 if voted_count.nil?

    candidates = db.query('SELECT name FROM candidates')
    if user.nil?
      return erb :vote, locals: { candidates: candidates, message: '個人情報に誤りがあります' }
    elsif user[:votes] < (params[:vote_count].to_i + voted_count.to_i)
      return erb :vote, locals: { candidates: candidates, message: '投票数が上限を超えています' }
    elsif params[:candidate].nil? || params[:candidate] == ''
      return erb :vote, locals: { candidates: candidates, message: '候補者を記入してください' }
    elsif candidate.nil?
      return erb :vote, locals: { candidates: candidates, message: '候補者を正しく記入してください' }
    elsif params[:keyword].nil? || params[:keyword] == ''
      return erb :vote, locals: { candidates: candidates, message: '投票理由を記入してください' }
    end

    params[:vote_count].to_i.times do
      countup candidate[:id]
      countup_user user[:id]
      countup_keyword candidate[:id], params[:keyword]
      countup_c_id candidate[:id]
      countup_parties candidate[:political_party]
      countup_sex candidate[:sex]
      countup_rank_parties candidate[:political_party]
      countup_rank_sex  candidate[:sex]
    end
    return erb :vote, locals: { candidates: candidates, message: '投票に成功しました' }
  end

  get '/initialize' do
    db_initialize
  end

  def countup c_id
    c_id = c_id.to_i
    redis.incr(c_id)
  end

  def fetch_count c_id
    redis.get(c_id)
  end

  def countup_user u_id
    u_id = "u_#{u_id}"
    redis.incr(u_id)
  end

  def fetch_count_user u_id
    u_id = "u_#{u_id}"
    redis.get(u_id)
  end

  def countup_keyword c_id, keyword
    key = "c_key_#{c_id}"
    redis.zincrby key, 1, keyword
  end

  def countup_c_id c_id
    key = "c_all_key"
    redis.zincrby key, 1, c_id
  end

  def fetch_top_c_id_top10
    redis.zrevrange "c_all_key", 0, 9
  end

  def fetch_top_c_id_last
    all = redis.zrevrange("c_all_key", 0, 99999)
    query = <<SQL
SELECT id, name, political_party, sex
FROM candidates
WHERE id NOT IN (?)
order by id asc
limit 1
SQL
    db.xquery(query, all).first
  end

  def countup_parties parties
    key = "p_key_#{parties}"
    redis.incr key
  end
  def countup_sex sex
    key = "s_key_#{sex}"
    redis.incr key
  end
  def get_parties parties
    key = "p_key_#{parties}"
    redis.get key
  end
  def get_sex parties
    key = "s_key_#{parties}"
    redis.get key
  end
  def countup_rank_parties parties
    key = "p_all_key"
    redis.zincrby key, 1, parties
  end
  def countup_rank_sex sex
    key = "s_all_key"
    redis.zincrby key, 1, sex
  end
  def fetch_top_parties
    redis.zrevrange "p_all_key", 0, 100
  end
  def fetch_top_sex
    redis.zrevrange "s_all_key", 0, 100
  end
end
