package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable}
import com.kakao.s2graph.core.{QueryParam, Vertex}

/**
 * Created by jojo on 1/22/16.
 */
class RedisVertexDeserializable extends StorageDeserializable[Vertex] {
  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[Vertex]): Vertex = ???
}
