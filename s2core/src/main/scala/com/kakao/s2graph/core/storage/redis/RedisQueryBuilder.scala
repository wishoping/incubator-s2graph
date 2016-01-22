package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.QueryBuilder
import com.kakao.s2graph.core.types._

import scala.collection.Map
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by june.kay on 2015. 12. 31..
 */
class RedisQueryBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends QueryBuilder[RedisGetRequest, Future[QueryRequestWithResult]](storage) {

  def buildRequest(queryRequest: QueryRequest): RedisGetRequest = ???

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean): Future[QueryRequestWithResult] = ???

  def fetch(queryRequest: QueryRequest, prevStepScore: Double, isInnerCall: Boolean, parentEdges: Seq[EdgeWithScore]): Future[QueryRequestWithResult] = ???

  def toCacheKeyBytes(request: RedisGetRequest): Array[Byte] = ???

  def fetches(queryRequestWithScoreLs: Seq[(QueryRequest, Double)], prevStepEdges: Map[VertexId, Seq[EdgeWithScore]]): Future[Seq[QueryRequestWithResult]] = ???
}
