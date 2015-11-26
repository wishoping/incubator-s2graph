package com.kakao.s2graph.core.cache


import java.util.concurrent.TimeUnit

import com.kakao.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseQueryBuilder}
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{QueryRequest, QueryResult}
import com.kakao.s2graph.core.storage.{SCache}
import com.typesafe.config.Config
import org.hbase.async.GetRequest
import spray.caching._
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

class RedisCache(config: Config, storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends SCache[QueryRequest, Future[Seq[QueryResult]]] {

//  implicit val akkaSystem = akka.actor.ActorSystem()
//
//  val instances = if (config.hasPath("redis.instances")) config.getStringList("redis.instances").toList else List("localhost")
//  val database = if (config.hasPath("redis.database")) config.getInt("redis.database") else 0
//  val clients = instances map { s =>
//    val sp = s.split(':')
//    val (host, port) = if (sp.length > 1) (sp(0), sp(1).toInt) else (sp(0), 6379)
//    RedisClient(host = host, port = port, db = Option(database))
//  } toVector
//  val poolSize = clients.size

//  private def shardKey(key: Any): Int = GraphUtil.murmur3Int(key.toString) % poolSize
//  def getClient(key: Any): RedisClient = clients.get(shardKey(key))

  val clients = new WithRedis(config)
  val builder = new AsynchbaseQueryBuilder(storage)

  val maxSize = 10000
  val ttl = Duration(100, TimeUnit.MILLISECONDS)
  val tti = Duration(50, TimeUnit.MILLISECONDS)
//  val cache = CacheBuilder.newBuilder()
//  .expireAfterAccess(100, TimeUnit.MILLISECONDS)
//  .expireAfterWrite(100, TimeUnit.MILLISECONDS)
//  .maximumSize(maxSize).build[java.lang.Long, Future[Seq[QueryResult]]]()

  val cache: Cache[Seq[QueryResult]] = LruCache(maxCapacity = maxSize, initialCapacity = maxSize, timeToLive = ttl, timeToIdle = tti)

  private def buildRequest(queryRequest: QueryRequest): GetRequest = builder.buildRequest(queryRequest)
  private def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = builder.toCacheKeyBytes(getRequest)

  private def toCacheKey(queryRequest: QueryRequest): Long =
    queryRequest.queryParam.toCacheKey(toCacheKeyBytes(buildRequest(queryRequest)))

  private def getBytes(value: Any): Array[Byte] = value.toString().getBytes("UTF-8")
  private def toTs(queryRequest: QueryRequest): Int = (queryRequest.queryParam.cacheTTLInMillis / 1000).toInt

  def redisGet(queryRequest: QueryRequest): Seq[QueryResult] = {
    val key = toCacheKey(queryRequest)
    clients.doBlockWithKey(key.toString) { jedis =>
      val v = jedis.get(getBytes(key))

      val ret =
        if (v == null) Nil
        else QueryResult.fromBytes(storage, queryRequest)(v, 0)

      logger.debug(s"redisGet: ${ret}")
      ret
    }
  }
  def redisPut(queryRequest: QueryRequest, queryResultLs: Seq[QueryResult]): String = {
    val key = toCacheKey(queryRequest)
    val bytes = QueryResult.toBytes(storage)(queryResultLs)
    clients.doBlockWithKey(key.toString) { jedis =>
      logger.debug(s"redisPut: $key, ${toTs(queryRequest)}, ${bytes.toList}")
      jedis.setex(getBytes(key), toTs(queryRequest), bytes)
    }
  }

  override def getIfPresent(queryRequest: QueryRequest): Future[Seq[QueryResult]] = {
    val key = toCacheKey(queryRequest)
    cache(key) { redisGet(queryRequest) }
  }

//  def remove(queryRequest: QueryRequest): Unit = cache.asMap().remove(toCacheKey(queryRequest))

  override def put(queryRequest: QueryRequest, queryResultLsFuture: Future[Seq[QueryResult]]): Unit = {
    queryResultLsFuture.onComplete {
      case Failure(ex) =>
        logger.error(s"put to redis failed.")
      case Success(queryResultLs) =>
        redisPut(queryRequest, queryResultLs)
    }
  }
}