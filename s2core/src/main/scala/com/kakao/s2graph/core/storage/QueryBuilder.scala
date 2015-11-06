package com.kakao.s2graph.core.storage

import com.google.common.cache.Cache
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.types.{LabelWithDirection, VertexId}
import scala.collection.{Map, Seq}
import scala.concurrent.{Future, ExecutionContext}

trait QueryBuilder[R, T] {

  def buildRequest(queryRequest: QueryRequest): R

  def getEdge(srcVertex: Vertex, tgtVertex: Vertex, queryParam: QueryParam, isInnerCall: Boolean)(implicit ex: ExecutionContext): T

  def fetch(queryRequest: QueryRequest,
            cacheOpt: Option[Cache[Integer, Seq[QueryResult]]])(implicit ex: ExecutionContext): T

  def toCacheKeyBytes(request: R): Array[Byte]

  def fetches(queryRequests: Seq[QueryRequest],
              prevStepEdges: Map[VertexId, Seq[EdgeWithScore]],
              cacheOpt: Option[Cache[Integer, Seq[QueryResult]]])(implicit ex: ExecutionContext): Future[Seq[QueryResult]]


  def fetchStep(queryResultsLs: Seq[QueryResult],
                q: Query,
                stepIdx: Int,
                cacheOpt: Option[Cache[Integer, Seq[QueryResult]]])(implicit ex: ExecutionContext): Future[Seq[QueryResult]] = {

    val prevStepOpt = if (stepIdx > 0) Option(q.steps(stepIdx - 1)) else None
    val prevStepThreshold = prevStepOpt.map(_.nextStepScoreThreshold).getOrElse(QueryParam.DefaultThreshold)
    val prevStepLimit = prevStepOpt.map(_.nextStepLimit).getOrElse(-1)
    val step = q.steps(stepIdx)
    val alreadyVisited =
      if (stepIdx == 0) Map.empty[(LabelWithDirection, Vertex), Boolean]
      else Graph.alreadyVisitedVertices(queryResultsLs)

    val groupedBy = queryResultsLs.flatMap { queryResult =>
      queryResult.edgeWithScoreLs.map { case edgeWithScore =>
        edgeWithScore.edge.tgtVertex -> edgeWithScore
      }
    }.groupBy { case (vertex, edgeWithScore) => vertex }

    val groupedByFiltered = for {
      (vertex, edgesWithScore) <- groupedBy
      aggregatedScore = edgesWithScore.map(_._2.score).sum if aggregatedScore >= prevStepThreshold
    } yield vertex -> aggregatedScore

    val prevStepTgtVertexIdEdges = for {
      (vertex, edgesWithScore) <- groupedBy
    } yield vertex.id -> edgesWithScore.map { case (vertex, edgeWithScore) => edgeWithScore }

    val nextStepSrcVertices = if (prevStepLimit >= 0) {
      groupedByFiltered.toSeq.sortBy(-1 * _._2).take(prevStepLimit)
    } else {
      groupedByFiltered.toSeq
    }

    val queryRequests = for {
      (vertex, prevStepScore) <- nextStepSrcVertices
      queryParam <- step.queryParams
    } yield QueryRequest(q, stepIdx, vertex, queryParam, prevStepScore, None, Nil, isInnerCall = false)

    Graph.filterEdges(fetches(queryRequests, prevStepTgtVertexIdEdges, cacheOpt), q, stepIdx, alreadyVisited)(ex)
  }

  def fetchStepFuture(queryResultLsFuture: Future[Seq[QueryResult]],
                      q: Query,
                      stepIdx: Int,
                      cacheOpt: Option[Cache[Integer, Seq[QueryResult]]])(implicit ex: ExecutionContext): Future[Seq[QueryResult]] = {
    for {
      queryResultLs <- queryResultLsFuture
      ret <- fetchStep(queryResultLs, q, stepIdx, cacheOpt)
    } yield ret
  }
}