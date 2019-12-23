require "rack/cache"
require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require "redis"
require 'json'
require 'active_support'
require 'active_support/core_ext'

module Ishocon2
  class AuthenticationError < StandardError; end
  class PermissionDenied < StandardError; end
end

class Ishocon2::WebApp < Sinatra::Base
  session_secret = ENV['ISHOCON2_SESSION_SECRET'] || 'showwin_happy'
  use Rack::Session::Cookie, key: 'rack.session', secret: session_secret
  set :erb, { escape_html: false }
  set :public_folder, File.expand_path('../public', __FILE__)
  set :protection, true

  @@bench_get_mode = false



  helpers do
    def redis
      return Thread.current[:ishocon2_redis] if Thread.current[:ishocon2_redis]
      client = Redis.new
      Thread.current[:ishocon2_redis] = client
      client
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

    def voice_of_supporter(candidate_ids)
      keys = []
      candidate_ids.each do |c_id|
        keys << "c_key_#{c_id}"
      end
      redis.zunionstore("c_ids_#{candidate_ids.join('-')}", keys)
      redis.zrevrange "c_ids_#{candidate_ids.join('-')}", 0, 9
    end

    def clear_nginx_cache
      system('rm -rf /var/cache/nginx/*')
    end
  end

  get '/' do
    cache_control :public, :max_age => 86400 if @@bench_get_mode

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

    erb  :index, locals: { candidates: candidates,
                          parties: parties,
                          sex_ratio: sex_ratio }
  end

  get '/candidates/:id' do
    @@bench_get_mode = true
    cache_control :public, :max_age => 86400
    return redirect '/' if params[:id].to_i > 30
    candidate = db.xquery('SELECT political_party, name, sex FROM candidates WHERE id = ? limit 1', params[:id]).first

    votes = fetch_count params[:id]
    keywords = voice_of_supporter([params[:id]])
    erb  :candidate, locals: { candidate: candidate,
                              votes: votes,
                              keywords: keywords }
  end

  get '/political_parties/:name' do
    @@bench_get_mode = true
    cache_control :public, :max_age => 86400

    votes = get_parties(params[:name]).to_i || 0
    candidates = db.xquery('SELECT id, name FROM candidates WHERE political_party = ?', params[:name])
    candidate_ids = candidates.map { |c| c[:id] }
    keywords = voice_of_supporter(candidate_ids)
    erb  :political_party, locals: { political_party: params[:name],
                                    votes: votes,
                                    candidates: candidates,
                                    keywords: keywords }
  end

  get '/vote' do
    cache_control :public, :max_age => 86400
    candidates = db.query('SELECT name FROM candidates')
    erb  :vote, locals: { candidates: candidates, message: '' }
  end

  post '/vote' do
    user = redis.get("user_#{params[:name]}#{params[:address]}#{params[:mynumber]}")
    if user.nil? || user.empty?
      user = db.xquery('SELECT * FROM users WHERE  name = ? AND address = ? AND mynumber = ?',
                       params[:name],
                       params[:address],
                       params[:mynumber]).first
      unless user.nil?
        redis.set("user_#{params[:name]}#{params[:address]}#{params[:mynumber]}", user.to_json)
      end
    else
      user = JSON.parse(user).with_indifferent_access
    end

    #TODO: ここ id で fetch できないかな？
    candidate = redis.get("candidate_fetch_#{params[:candidate]}")
    if candidate.nil? || candidate.empty?
      candidate = db.xquery('SELECT * FROM candidates WHERE name = ? limit 1', params[:candidate]).first
      unless candidate.nil?
        redis.set("candidate_fetch_#{params[:candidate]}", candidate.to_json)
      end
    else
      candidate = JSON.parse(candidate).with_indifferent_access
    end

    voted_count = user.nil? ? 0 : fetch_count_user(user[:id])
    voted_count = 0 if voted_count.nil?

    candidates = fetch_candidates
    if user.nil?
      view = redis.get('vote_err_1')
      if view.nil?
        view = erb  :vote, locals: { candidates: candidates, message: '個人情報に誤りがあります' }
        redis.set('vote_err_1', view)
      end
      return view
    elsif user[:votes] < (params[:vote_count].to_i + voted_count.to_i)
      view = redis.get('vote_err_2')
      if view.nil?
        view = erb  :vote, locals: { candidates: candidates, message: '投票数が上限を超えています' }
        redis.set('vote_err_1', view)
      end
      return view
    elsif params[:candidate].nil? || params[:candidate] == ''
      view = redis.get('vote_err_3')
      if view.nil?
        view = erb  :vote, locals: { candidates: candidates, message: '候補者を記入してください' }
        redis.set('vote_err_1', view)
      end
      return view
    elsif candidate.nil?
      view = redis.get('vote_err_4')
      if view.nil?
        view = erb  :vote, locals: { candidates: candidates, message: '候補者を正しく記入してください' }
        redis.set('vote_err_1', view)
      end
      return view
    elsif params[:keyword].nil? || params[:keyword] == ''
      view = redis.get('vote_err_5')
      if view.nil?
        view = erb  :vote, locals: { candidates: candidates, message: '投票理由を記入してください' }
        redis.set('vote_err_1', view)
      end
      return view
    end

    count = params[:vote_count].to_i
    countup candidate[:id], count
    countup_user user[:id], count
    countup_keyword candidate[:id], params[:keyword], count
    countup_c_id candidate[:id], count
    countup_parties candidate[:political_party], count
    countup_sex candidate[:sex], count
    countup_rank_parties candidate[:political_party], count
    countup_rank_sex  candidate[:sex], count
    view = redis.get('vote_success')
    if view.nil?
      view = erb  :vote, locals: { candidates: candidates, message: '投票に成功しました' }
      redis.set('vote_success', view)
    end
    return view
  end

  get '/initialize' do
    clear_nginx_cache
    redis.flushdb
    #set_all_candidates_ranking
  end

  def fetch_candidates
    @@candidates ||= db.query('SELECT name FROM candidates')
  end

  def countup c_id, count
    c_id = c_id.to_i
    redis.incrby c_id, count
  end

  def fetch_count c_id
    redis.get(c_id)
  end

  def countup_user u_id, count
    u_id = "u_#{u_id}"
    redis.incrby u_id, count
  end

  def fetch_count_user u_id
    u_id = "u_#{u_id}"
    redis.get(u_id)
  end

  def countup_keyword c_id, keyword, count
    key = "c_key_#{c_id}"
    redis.zincrby key, count, keyword
  end

  def countup_c_id c_id, count
    key = "c_all_key"
    redis.zincrby key, count, c_id
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

  def countup_parties parties, count
    key = "p_key_#{parties}"
    redis.incrby key, count
  end
  def countup_sex sex, count
    key = "s_key_#{sex}"
    redis.incrby key, count
  end
  def get_parties parties
    key = "p_key_#{parties}"
    redis.get key
  end
  def get_sex parties
    key = "s_key_#{parties}"
    redis.get key
  end
  def countup_rank_parties parties, count
    key = "p_all_key"
    redis.zincrby key, count, parties
  end
  def countup_rank_sex sex, count
    key = "s_all_key"
    redis.zincrby key, count, sex
  end
  def fetch_top_parties
    redis.zrevrange "p_all_key", 0, 100
  end
  def fetch_top_sex
    redis.zrevrange "s_all_key", 0, 100
  end

#  def set_all_candidates_ranking
#    all_candidates.each do |candidate|
#      redis.zadd('c_all_key', 0, candidate[:id])
#    end
#  end
#
#  def all_candidates
#    @@candidates ||= fetch_all_candidates
#  end
#
#  def fetch_all_candidates
#    query = <<SQL
#SELECT id, name, political_party, sex
#FROM candidates
#order by id asc
#SQL
#    res = db.xquery(query, all)
#    data = []
#    res.each do |val|
#      data << val
#    end
#    data
#  end
end
