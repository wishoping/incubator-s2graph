package com.kakao.s2graph.core.storage.hbase

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache.CacheBuilder
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.QueryBuilder
import com.kakao.s2graph.core.types._
import com.kakao.s2graph.core.utils.{Extensions, logger}
import com.stumbleupon.async.Deferred
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.GetRequest

import scala.collection.JavaConversions._
import scala.collection.{Map, Seq}
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.Success

class AsynchbaseQueryBuilder(storage: AsynchbaseStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[GetRequest, Future[QueryRequestWithResult]](storage) {

  import Extensions.DeferOps



  override def buildRequest(queryRequest: QueryRequest): GetRequest = {
    val srcVertex = queryRequest.vertex
    //    val tgtVertexOpt = queryRequest.tgtVertexOpt
    val edgeCf = HSerializable.edgeCf

    val queryParam = queryRequest.queryParam
    val tgtVertexIdOpt = queryParam.tgtVertexInnerIdOpt
    val label = queryParam.label
    val labelWithDir = queryParam.labelWithDir
    val (srcColumn, tgtColumn) = label.srcTgtColumn(labelWithDir.dir)
    val (srcInnerId, tgtInnerId) = tgtVertexIdOpt match {
      case Some(tgtVertexId) => // _to is given.
        /** we use toInvertedEdgeHashLike so dont need to swap src, tgt */
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        val tgt = InnerVal.convertVersion(tgtVertexId, tgtColumn.columnType, label.schemaVersion)
        (src, tgt)
      case None =>
        val src = InnerVal.convertVersion(srcVertex.innerId, srcColumn.columnType, label.schemaVersion)
        (src, src)
    }

    val (srcVId, tgtVId) = (SourceVertexId(srcColumn.id.get, srcInnerId), TargetVertexId(tgtColumn.id.get, tgtInnerId))
    val (srcV, tgtV) = (Vertex(srcVId), Vertex(tgtVId))
    val currentTs = System.currentTimeMillis()
    val propsWithTs =  Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(currentTs, label.schemaVersion), currentTs)).toMap
    val edge = Edge(srcV, tgtV, labelWithDir, propsWithTs = propsWithTs)

    val get = if (tgtVertexIdOpt.isDefined) {
      val snapshotEdge = edge.toSnapshotEdge
      val kv = storage.snapshotEdgeSerializer(snapshotEdge).toKeyValues.head
      new GetRequest(label.hbaseTableName.getBytes, kv.row, edgeCf, kv.qualifier)
    } else {
      val indexedEdgeOpt = edge.edgesWithIndex.find(e => e.labelIndexSeq == queryParam.labelOrderSeq)
      assert(indexedEdgeOpt.isDefined)

      val indexedEdge = indexedEdgeOpt.get
      val kv = storage.indexEdgeSerializer(indexedEdge).toKeyValues.head
      val table = label.hbaseTableName.getBytes
      val rowKey = kv.row
      val cf = edgeCf
      new GetRequest(table, rowKey, cf)
    }

    val (minTs, maxTs) = queryParam.duration.getOrElse((0L, Long.MaxValue))

    get.maxVersions(1)
    get.setFailfast(true)
    get.setMaxResultsPerColumnFamily(queryParam.limit)
    get.setRowOffsetPerColumnFamily(queryParam.offset)
    get.setMinTimestamp(minTs)
    get.setMaxTimestamp(maxTs)
    get.setTimeout(queryParam.rpcTimeoutInMillis)

    if (queryParam.columnRangeFilter != null) get.setFilter(queryParam.columnRangeFilter)

    get
  }

  override def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryRequestWithResult] = {
    //TODO:
    val _queryParam = queryParam.tgtVertexInnerIdOpt(Option(tgtVertex.innerId))
    val q = Query.toQuery(Seq(srcVertex), _queryParam)
    val queryRequest = QueryRequest(q, 0, srcVertex, _queryParam)
    fetch(queryRequest, 1.0, isInnerCall = true, parentEdges = Nil)
  }

  val maxSize = 100000
  val expireCount = 10
  val cache = CacheBuilder.newBuilder()
  .expireAfterAccess(20, TimeUnit.MILLISECONDS)
  .expireAfterWrite(20, TimeUnit.MILLISECONDS)
  .maximumSize(maxSize).build[java.lang.Long, (AtomicInteger, Future[QueryRequestWithResult])]()

  override def fetch(queryRequest: QueryRequest,
                     prevStepScore: Double,
                     isInnerCall: Boolean,
                     parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = {

    def fetchInner: Future[QueryRequestWithResult] = {
      val request = buildRequest(queryRequest)
      val keyBytes = toCacheKeyBytes(request)
      val cacheKey = queryRequest.queryParam.toCacheKey(keyBytes)
      val promise = Promise[QueryRequestWithResult]
      cache.asMap().putIfAbsent(cacheKey, (new AtomicInteger(0), promise.future)) match {
        case null =>
          val future = storage.client.get(request) withCallback { kvs =>
            val edgeWithScores = storage.toEdges(kvs.toSeq, queryRequest.queryParam, prevStepScore, isInnerCall, parentEdges)
            QueryRequestWithResult(queryRequest, QueryResult(edgeWithScores))
          } recoverWith { ex =>
            logger.error(s"fetchQueryParam failed. fallback return.", ex)
            QueryRequestWithResult(queryRequest, QueryResult(isFailure = true))
          } toFuture

          future onComplete {
            case Success(queryRequestWithResult) =>
              promise.success(queryRequestWithResult)
//              cache.asMap().remove(cacheKey)
          }
          future
        case (hitCount, existingFuture) =>
          val newHitCount = hitCount.incrementAndGet()
          if (newHitCount > expireCount) cache.asMap().remove(cacheKey)
          existingFuture
      }
    }

    val (query, stepIdx, vertex, queryParam) = QueryRequest.unapply(queryRequest).get
    storage.cacheOpt match {
      case None => fetchInner
      case Some(cache) =>
//        def setCacheAfterFetch: Future[QueryRequestWithResult] =
//          fetchInner.map { queryResult: QueryRequestWithResult =>
//            cache.put(queryRequest, Seq(queryResult.queryResult))
//            queryResult
//          }

        if (queryParam.cacheTTLInMillis < 0) fetchInner
        else {
          val cacheTTL = queryParam.cacheTTLInMillis
          val cacheValLsFuture = cache.getIfPresent(queryRequest)
          cacheValLsFuture.flatMap { cacheValLs =>
//            logger.debug(s"[CACHE]: ${cacheValLs.nonEmpty}, ${System.currentTimeMillis()}, ${cacheValLs.headOption.map(_.timestamp)}")

            if (cacheValLs.nonEmpty && System.currentTimeMillis() < cacheValLs.head.timestamp + cacheTTL) {
//              logger.debug(s"[Hit]")
              Future.successful(QueryRequestWithResult(queryRequest, cacheValLs.head))
            } else {
//              logger.debug(s"[Miss]")
              val future = fetchInner
              cache.put(queryRequest, future.map(value => Seq(value.queryResult)))
              future
            }
          }
//
        }
    }
  }

  override def toCacheKeyBytes(getRequest: GetRequest): Array[Byte] = {
    var bytes = getRequest.key()
    Option(getRequest.family()).foreach(family => bytes = Bytes.add(bytes, family))
    Option(getRequest.qualifiers()).foreach { qualifiers =>
      qualifiers.filter(q => Option(q).isDefined).foreach { qualifier =>
        bytes = Bytes.add(bytes, qualifier)
      }
    }
//    if (getRequest.family() != null) bytes = Bytes.add(bytes, getRequest.family())
//    if (getRequest.qualifiers() != null) getRequest.qualifiers().filter(_ != null).foreach(q => bytes = Bytes.add(bytes, q))
    bytes
  }


  override def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)],
                       prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = {
    val defers: Seq[Future[QueryRequestWithResult]] = for {
      (queryRequest, prevStepScore) <- queryRequestWithScoreLs
    } yield {
      val prevStepEdgesOpt = prevStepEdges.get(queryRequest.vertex.id)
      if (prevStepEdgesOpt.isEmpty) throw new RuntimeException("miss match on prevStepEdge and current GetRequest")

      val parentEdges = for {
        parentEdge <- prevStepEdgesOpt.get
      } yield parentEdge

      fetch(queryRequest, prevStepScore, isInnerCall = true, parentEdges)
    }
//    val grouped: Deferred[util.ArrayList[QueryRequestWithResult]] = Deferred.group(defers)
//    grouped withCallback { queryResults: util.ArrayList[QueryRequestWithResult] =>
//      queryResults.toIndexedSeq
//    } toFuture
    Future.sequence(defers)
  }
}
