package com.kakao.s2graph.core.cache


import java.util.concurrent.TimeUnit

import akka.util.ByteString
import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core.storage.hbase.{AsynchbaseStorage, AsynchbaseQueryBuilder}
import com.kakao.s2graph.core.utils.logger
import com.kakao.s2graph.core.{GraphUtil, QueryRequest, QueryResult}
import com.kakao.s2graph.core.storage.{SCache}
import com.typesafe.config.Config
import org.hbase.async.GetRequest
import redis.RedisClient
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

class RedisCache(config: Config, storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends SCache[QueryRequest, Future[Seq[QueryResult]]] {

  implicit val akkaSystem = akka.actor.ActorSystem("blocking")

  val instances = if (config.hasPath("redis.instances")) config.getStringList("redis.instances").toList else List("localhost")
  val database = if (config.hasPath("redis.database")) config.getInt("redis.database") else 0
  val clients = instances map { s =>
    val sp = s.split(':')
    val (host, port) = if (sp.length > 1) (sp(0), sp(1).toInt) else (sp(0), 6379)
    RedisClient(host = host, port = port, db = Option(database))
  } toVector
  val poolSize = clients.size

  private def shardKey(key: Any): Int = GraphUtil.murmur3Int(key.toString) % poolSize
  def getClient(key: Any): RedisClient = clients.get(shardKey(key))

  val builder = new AsynchbaseQueryBuilder(storage)

  val maxSize = 100000
  val cache = CacheBuilder.newBuilder()
  .expireAfterAccess(1000, TimeUnit.MILLISECONDS)
  .expireAfterWrite(1000, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, Future[Seq[QueryResult]]]()

  private def buildRequest(queryRequest: QueryRequest): GetRequest = builder.buildRequest(queryRequest)
  private def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = builder.toCacheKeyBytes(getRequest)

  private def toCacheKey(queryRequest: QueryRequest): Long =
    queryRequest.queryParam.toCacheKey(toCacheKeyBytes(buildRequest(queryRequest)))

  private def getBytes(value: Any): Array[Byte] = value.toString().getBytes("UTF-8")
  private def toTs(queryRequest: QueryRequest): Int = (queryRequest.queryParam.cacheTTLInMillis / 1000).toInt

  override def getIfPresent(queryRequest: QueryRequest): Future[Seq[QueryResult]] = {
    val key = toCacheKey(queryRequest)
    val promise = Promise[Seq[QueryResult]]
    cache.asMap().putIfAbsent(key, promise.future) match {
      case null =>
//        logger.debug(s"[MISS]: FutureCache.")
        val future = getClient(key).get(key.toString).map { valueOpt =>
          valueOpt match {
            case None => Nil
            case Some(ls) => QueryResult.fromBytes(storage, queryRequest)(ls.toArray, 0)
          }
        }
        future onComplete {
          case Success(value) =>
            promise.success(value)
            cache.asMap().remove(key)
          case Failure(ex) =>
//            logger.error(s"getIfPresent failed.")
            cache.asMap().remove(key)
//            cache.asMap().remove(key, promise.future)
        }
//        future.onComplete { valueOpt =>
//          promise.complete(valueOpt)
//          if (valueOpt.isFailure) cache.asMap().remove(key, promise.future)
//        }

        future
      case existingFuture =>
//        logger.debug(s"[HIT]: FutureCache.")
        existingFuture
    }

  }

  def remove(queryRequest: QueryRequest): Unit = cache.asMap().remove(toCacheKey(queryRequest))

  override def put(queryRequest: QueryRequest, queryResultLsFuture: Future[Seq[QueryResult]]): Unit = {
//    val key = toCacheKey(queryRequest)
//    queryResultLsFuture onComplete {
//      case Success(queryResultLs) =>
//        val bytes = QueryResult.toBytes(storage)(queryResultLs)
//
//
//        val future = getClient(key).setex(key.toString, toTs(queryRequest), ByteString(bytes)).flatMap { ret =>
//          getClient(key).get(key.toString).map { valueOpt =>
//            valueOpt match {
//              case None => Nil
//              case Some(ls) => QueryResult.fromBytes(storage, queryRequest)(ls.toArray, 0)
//            }
//          }
//        }
//        cache.asMap().put(key, future)
//      case Failure(ex) =>
//        logger.error(s"fetch failed.")
//    }
    queryResultLsFuture.onComplete { queryResultLsTry =>
      if (queryResultLsTry.isSuccess) {
        val key = toCacheKey(queryRequest)
        val queryResultLs = queryResultLsTry.get
        val bytes = QueryResult.toBytes(storage)(queryResultLs)

        getClient(key).setex(key.toString, toTs(queryRequest), ByteString(bytes)) onComplete {
          case Success(ret) =>
            cache.asMap().remove(key)
          case Failure(ex) =>
            logger.error(s"set to redis failed.")
        }
//        clients.doBlockWithKey(key.toString) { jedis =>
//          jedis.setex(getBytes(key), toTs(queryRequest), bytes)
//        }
      }
    }
  }
}