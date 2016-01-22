package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core._
import com.kakao.s2graph.core.storage.{SKeyValue, MutationBuilder}
import scala.concurrent.ExecutionContext

/**
 * Created by jojo on 1/22/16.
 */
class RedisMutationBuilder(storage: RedisStorage)(implicit ec: ExecutionContext)
  extends MutationBuilder[RedisRPC](storage) {

  /** operation that needs to be supported by backend persistent storage system */
  override def put(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  override def buildPutsAsync(indexedEdge: IndexEdge): Seq[RedisRPC] = ???

  /** Vertex */
  override def buildPutsAsync(vertex: Vertex): Seq[RedisRPC] = ???

  override def buildDeleteBelongsToId(vertex: Vertex): Seq[RedisRPC] = ???

  override def increments(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  override def increment(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  override def buildDeleteAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = ???

  override def buildDeleteAsync(vertex: Vertex): Seq[RedisRPC] = ???

  override def buildVertexPutsAsync(edge: Edge): Seq[RedisRPC] = ???

  override def delete(kvs: Seq[SKeyValue]): Seq[RedisRPC] = ???

  /** IndexEdge */
  override def buildIncrementsAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] = ???

  override def buildDeletesAsync(indexedEdge: IndexEdge): Seq[RedisRPC] = ???

  override def snapshotEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  override def buildIncrementsCountAsync(indexedEdge: IndexEdge, amount: Long): Seq[RedisRPC] = ???

  /** EdgeMutate */
  override def indexedEdgeMutations(edgeMutate: EdgeMutate): Seq[RedisRPC] = ???

  /** SnapshotEdge */
  override def buildPutAsync(snapshotEdge: SnapshotEdge): Seq[RedisRPC] = ???
}
