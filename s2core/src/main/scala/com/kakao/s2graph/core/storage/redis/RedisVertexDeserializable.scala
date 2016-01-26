package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.storage.{CanSKeyValue, StorageDeserializable}
import com.kakao.s2graph.core.types.{InnerVal, InnerValLike, VertexId}
import com.kakao.s2graph.core.{GraphUtil, QueryParam, Vertex}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * Created by june.kay on 2016. 1. 13..
 */
class RedisVertexDeserializable extends StorageDeserializable[Vertex]{

  override def fromKeyValues[T: CanSKeyValue](queryParam: QueryParam,
    _kvs: Seq[T],
    version: String,
    cacheElementOpt: Option[Vertex]): Vertex = {

    val kvs = _kvs.map { kv => implicitly[CanSKeyValue[T]].toSKeyValue(kv) }

    val kv = kvs.head
    val (vertexId, _) = VertexId.fromBytes(kv.row, -GraphUtil.bytesForMurMurHash, kv.row.length, version) // no hash bytes => offset: -2

    var maxTs = Long.MinValue
    val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
    val belongLabelIds = new ListBuffer[Int]

    for {
      kv <- kvs
    } {
      val propKey = Bytes.toInt(kv.qualifier)
      // TODO :: What the...
      //        if (kv.qualifier.length == 1) kv.qualifier.head.toInt
      //        else Bytes.toInt(kv.qualifier)

      val ts = kv.timestamp
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val v = kv.value
        val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

