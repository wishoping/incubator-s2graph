package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.{IndexEdge, QueryParam}
import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable}

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Jan/07.
 */
class RedisIndexEdgeDeserializable extends StorageDeserializable[IndexEdge] {
  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam, kvs: Seq[T], version: String, cacheElementOpt: Option[IndexEdge]): IndexEdge = ???
}
