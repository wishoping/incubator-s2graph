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
      var offset = 0
      val propKey = Bytes.toInt(kv.value)
      offset += 4
      // TODO :: What the...
      //        if (kv.qualifier.length == 1) kv.qualifier.head.toInt
      //        else Bytes.toInt(kv.qualifier)

//      val ts = kv.timestamp
      val ts = Bytes.toLong(kv.value, offset, 8)
      // read Long value
      offset += 8
      if (ts > maxTs) maxTs = ts

      if (Vertex.isLabelId(propKey)) {
        belongLabelIds += Vertex.toLabelId(propKey)
      } else {
        val (value, _) = InnerVal.fromBytes(kv.value, offset, kv.value.length, version)
        propsMap += (propKey -> value)
      }
    }
    assert(maxTs != Long.MinValue)
    Vertex(vertexId, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds)
  }
}

