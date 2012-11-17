require 'librato/metrics'
require 'scrolls'
require 'redis'
require 'locksmith/dynamodb'

require 'l2met/db'
require 'l2met/utils'
require 'l2met/config'
require 'l2met/stats'

module L2met
  module DBOutlet
    extend self
    INTERVAL = 10

    def start
      max = Config.num_dboutlets
      loop do
        bucket = Utils.trunc_time(Time.now) - 120
        Thread.new do
          max.times.each do |p|
            lock_name = "#{Config.app_name}.dboutlet.#{p}"
            Locksmith::Dynamodb.lock(lock_name, ttl: 60) do
              snapshot(p, max, bucket)
            end
          end
        end
        sleep(INTERVAL)
      end
    end

    def snapshot(partition, max, bucket)
      Utils.measure('outlet.snapshot') do
        # mkey:bucket:uuid
        Utils.measure('outlet.redis.key-scan') do
          redis.keys("*:#{bucket}:*")
        end.select do |key|
          Integer(key.split(':')[0]) % max == partition
        end.map do |key|
          mkey = key.split(':')[0]
          Utils.measure('outlet.redis.get') do
            redis.hgetall(key).tap {redis.del(key)}.merge('mkey' => mkey)
          end
        end.group_by do |metric|
          metric["consumer"]
        end.each do |consumer_id, metrics|
          consumer = DB["consumers"].at(consumer_id).attributes
          client = build_client(consumer["email"], consumer["token"])
          queue =  Librato::Metrics::Queue.new(client: client)
          format_metrics(metrics).each {|m| queue.add(m)}
          if queue.length > 0
            log(at: 'librato.submit', length: queue.length)
            Utils.measure('librato.submit') {queue.submit}
          end
        end
      end
    end

    def format_metrics(metrics)
      metrics.group_by do |metric|
        metric['mkey']
      end.map do |mkey, metrics|
        s = metrics.sample
        opts = {source: s["source"],
          type: "gauge",
          attributes: {display_units_long: s["label"]},
          measure_time: s["time"].to_i}
        log(fn: __method__, at: "process", metric: s["name"])
        case s["type"]
        when "counter"
          val = metrics.map {|m| Float(m["value"])}.reduce(:+)
          name = [s["name"], 'count'].join(".")
          {name => opts.merge(value: val)}
        when "last"
          val = Float(metrics.last["value"])
          name = [s["name"], 'last'].join(".")
          {name => opts.merge(value: val)}
        when "list"
          Stats.aggregate(metrics).map do |stat, val|
            name = [s["name"], stat].map(&:to_s).join(".")
            {name => opts.merge(value: val)}
          end
        end
      end.flatten.compact
    end

    def build_client(email, token)
      Librato::Metrics::Client.new.tap do |c|
        c.authenticate(email, token)
      end
    end

    def redis
      @redis ||= Redis.new(url: Config.redis_url)
    end

    def log(data, &blk)
      Scrolls.log({ns: "dboutlet"}.merge(data), &blk)
    end

  end
end
