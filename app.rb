require "rack/cache"
require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require 'dalli'
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

  options = { :namespace => "app_v1", :compress => true }
  dc = Dalli::Client.new('localhost:11211', options)


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
      return redis.zrevrange "c_ids_#{candidate_ids.join('-')}", 0, 9
      #OnMemory.instance.fetch_top10(candidate_ids)
#      query = <<SQL
#SELECT keyword
#FROM votes
#WHERE candidate_id IN (?)
#GROUP BY keyword
#ORDER BY COUNT(*) DESC
#LIMIT 10
#SQL
#      db.xquery(query, candidate_ids).map { |a| a[:keyword] }
    end

    def db_initialize
      clear_nginx_cache
      memcache_flush
      db.query('DELETE FROM votes')
      OnMemory.instance.clear
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
    #cache_control :public, :max_age => 86400
    candidates = []


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
    #results = election_results
    #results.each_with_index do |r, i|
      # 上位10人と最下位のみ表示
      #candidates.push(r) if i < 10 || 28 < i
    #end

    parties_set = db.query('SELECT political_party FROM candidates GROUP BY political_party')
    parties = {}
    parties_set.each { |a| parties[a[:political_party]] = 0 }
    #results.each do |r|
    #  #parties[r[:political_party]] += r[:count] || 0
    #end

    parties_rank = fetch_top_parties
    parties_rank.each do |r|
      parties[r] += get_parties(r).to_i || 0
    end

    sex_ratio = { '男': 0, '女': 0 }
    #results.each do |r|
    #  #sex_ratio[r[:sex].to_sym] += r[:count] || 0
    #end
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
    candidate = dc.get("candidate_#{params[:id]}")
    if candidate.nil?
      candidate = db.xquery('SELECT political_party, name, sex FROM candidates WHERE id = ?', params[:id]).first
      dc.set("candidate_#{params[:id]}", candidate)
    end
    return redirect '/' if candidate.nil?

    votes = dc.get("candidate_votes_#{params[:id]}")
    if votes.nil?
      #votes = OnMemory.instance.fetch_vote_count params[:id]
      votes = fetch_count params[:id]
      #votes = db.xquery('SELECT COUNT(candidate_id) AS count FROM votes WHERE candidate_id = ?', params[:id]).first[:count]
      dc.set("candidate_votes_#{params[:id]}", votes)
    end
    keywords = voice_of_supporter([params[:id]])
    erb :candidate, locals: { candidate: candidate,
                              votes: votes,
                              keywords: keywords }
  end

  get '/political_parties/:name' do
    cache_control :public, :max_age => 86400
    votes = 0
    election_results.each do |r|
      votes += r[:count] || 0 if r[:political_party] == params[:name]
    end
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
    candidate = db.xquery('SELECT * FROM candidates WHERE name = ?', params[:candidate]).first

    voted_count = user.nil? ? 0 : fetch_count_user(user[:id])
    voted_count = 0 if voted_count.nil?
    #voted_count =
    #  user.nil? ? 0 : db.xquery('SELECT COUNT(id) AS count FROM votes WHERE user_id = ?', user[:id]).first[:count]

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
      #OnMemory.instance.add user[:id], candidate[:id], params[:keyword]
      countup candidate[:id]
      countup_user user[:id]
      countup_keyword candidate[:id], params[:keyword]
      countup_c_id candidate[:id]
      countup_parties candidate[:political_party]
      countup_sex candidate[:sex]
      countup_rank_parties candidate[:political_party]
      countup_rank_sex  candidate[:sex]
      result = db.xquery('INSERT INTO votes (user_id, candidate_id, keyword) VALUES (?, ?, ?)',
                user[:id],
                candidate[:id],
                params[:keyword])
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

require 'singleton'
class OnMemory
  include Singleton

  def initialize
    @votes = []
    @votes_candidate = {}
  end

  def clear
    @votes = []
    @votes_candidate = {}
  end

  def add user_id, candidate_id, keyword
    candidate_id = candidate_id.to_i
    user_id = user_id.to_i
    @votes << {
      user_id: user_id,
      candidate_id: candidate_id,
      keyword: keyword
    }
    if @votes_candidate[candidate_id].nil?
      @votes_candidate[candidate_id] = {}
    end
    if @votes_candidate[candidate_id][keyword].nil?
      @votes_candidate[candidate_id][keyword] = 1
    else
      @votes_candidate[candidate_id][keyword] += 1
    end
  end

  def fetch_top10 candidate_ids
    keywords = {}
    candidate_ids.each do |c_id|
      next if @votes_candidate[c_id].nil?
      @votes_candidate[c_id].each do |key, count|
        if keywords[key].nil?
          keywords[key] = 0
        end
        keywords[key] += count
      end
    end
    sorted = keywords.sort_by { |_, v| v }.reverse.to_h
    sorted.take(10)
  end

  def fetch_vote_count candidate_id
    candidate_id = candidate_id.to_i
    return 0 if @votes_candidate[candidate_id].nil?
    total_count = 0
    @votes_candidate[candidate_id].each do |key, count|
      total_count += count
    end
    total_count
  end
end
