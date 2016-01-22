package com.kakao.s2graph.core.storage.redis

import com.google.common.cache.Cache
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{StorageSerializable, Storage}
import com.typesafe.config.Config

import scala.concurrent.{Future, ExecutionContext}

/**
 * Redis storage handler class
 *
 * Created by june.kay on 2015. 12. 31..
 *
 */
class RedisStorage(override val config: Config, vertexCache: Cache[Integer, Option[Vertex]])
                  (implicit ec: ExecutionContext) extends Storage(config) {

  // initialize just once ( so we use `val` not `def` ) -> validate once
  val cacheOpt = None

  val snapshotEdgeDeserializer = new RedisSnapshotEdgeDeserializable
  val indexEdgeDeserializer = new RedisIndexEdgeDeserializable
  val vertexDeserializer = new RedisVertexDeserializable
  val queryBuilder = new RedisQueryBuilder(this)(ec)
  val mutationBuilder = new RedisMutationBuilder(this)(ec)

  override def createTable(zkAddr: String, tableName: String, cfs: List[String], regionMultiplier: Int, ttl: Option[Int], compressionAlgorithm: String): Unit = ???

  // Serializer/Deserializer
  def indexEdgeSerializer(indexedEdge: IndexEdge): StorageSerializable[IndexEdge] = ???

  def snapshotEdgeSerializer(snapshotEdge: SnapshotEdge): StorageSerializable[SnapshotEdge] = ???

  def vertexSerializer(vertex: Vertex): StorageSerializable[Vertex] = ???

  override def checkEdges(params: Seq[(Vertex, Vertex, QueryParam)]): Future[Seq[QueryRequestWithResult]] = ???

  override def flush(): Unit = ???

  override def vertexCacheOpt: Option[Cache[Integer, Option[Vertex]]] = ???


  // Interface
  override def getEdges(q: Query): Future[Seq[QueryRequestWithResult]] = ???

  override def deleteAllAdjacentEdges(srcVertices: List[Vertex], labels: Seq[Label], dir: Int, ts: Long): Future[Boolean] = ???

  override def incrementCounts(edges: Seq[Edge]): Future[Seq[(Boolean, Long)]] = ???

  override def mutateVertex(vertex: Vertex, withWait: Boolean): Future[Boolean] = ???

  override def mutateEdge(edge: Edge, withWait: Boolean): Future[Boolean] = ???

  override def getVertices(vertices: Seq[Vertex]): Future[Seq[Vertex]] = ???
}