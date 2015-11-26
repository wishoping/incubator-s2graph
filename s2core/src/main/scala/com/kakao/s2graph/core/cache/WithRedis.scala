package com.kakao.s2graph.core.cache

import com.kakao.s2graph.core.GraphUtil
import com.typesafe.config.Config
import redis.clients.jedis.exceptions.JedisException
import redis.clients.jedis.{Protocol, Jedis, JedisPool, JedisPoolConfig}
import scala.collection.JavaConversions._

class WithRedis(config: Config) {
  val instances = if (config.hasPath("redis.instances")) config.getStringList("redis.instances").toList else List("localhost")
  val Database = if (config.hasPath("redis.database")) config.getInt("redis.database") else 0

  val REDIS_INSTANCES = instances map { s =>
    val sp = s.split(':')
    (sp(0), if (sp.length > 1) sp(1).toInt else 6379)
  }

  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(150)
  poolConfig.setMaxIdle(50)
  poolConfig.setMaxWaitMillis(200)


  val jedisPools = REDIS_INSTANCES.map { case (host, port) =>
    new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, Database)
  }

  def getBucketIdx(key: String): Int = {
    GraphUtil.murmur3Int(key) % jedisPools.size
  }

  def doBlockWithIndex[T](idx: Int)(f: Jedis => T): T = {
    val pool = jedisPools(idx)

    var jedis: Jedis = null

    try {
      jedis = pool.getResource

      f(jedis)
    }
    catch {
      case e: JedisException =>
        pool.returnBrokenResource(jedis)

        jedis = null
        throw e
    }
    finally {
      if (jedis != null) {
        pool.returnResource(jedis)
      }
    }
  }

  def doBlockWithKey[T](key: String)(f: Jedis => T): T = {
    doBlockWithIndex(getBucketIdx(key))(f)
  }
}

