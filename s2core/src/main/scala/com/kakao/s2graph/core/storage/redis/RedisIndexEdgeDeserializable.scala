package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.{IndexEdge, QueryParam}
import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable}

/**
 * Created by jojo on 1/22/16.
 */
class RedisIndexEdgeDeserializable extends StorageDeserializable[IndexEdge] {
  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[IndexEdge]): IndexEdge = ???
}
