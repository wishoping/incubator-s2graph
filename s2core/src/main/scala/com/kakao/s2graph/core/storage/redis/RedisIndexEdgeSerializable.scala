package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.mysqls.LabelMeta
import com.kakao.s2graph.core.storage.{SKeyValue, StorageSerializable}
import com.kakao.s2graph.core.types.VertexId
import com.kakao.s2graph.core.types.v2.InnerVal
import com.kakao.s2graph.core.{GraphUtil, IndexEdge}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author Junki Kim (wishoping@gmail.com) and Hyunsung Jo (hyunsung.jo@gmail.com) on 2016/Jan/07.
 */
class RedisIndexEdgeSerializable(indexEdge: IndexEdge) extends StorageSerializable[IndexEdge]{
  import StorageSerializable._

  val label = indexEdge.label

  val idxPropsMap = indexEdge.orders.toMap
  val idxPropsBytes = propsToBytes(indexEdge.orders)

  def toKeyValues: Seq[SKeyValue] = {
    println(s"<< [RedisIndexEdgeSerializable:toKeyValues] enter")
    val _srcIdBytes = VertexId.toSourceVertexId(indexEdge.srcVertex.id).bytes
    val srcIdBytes = _srcIdBytes.takeRight(_srcIdBytes.length - GraphUtil.bytesForMurMurHash)
    val labelWithDirBytes = indexEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(indexEdge.labelIndexSeq, isInverted = false)

    println(s"\t<< [RedisIndexEdgeSerializable:toKeyValues] src[${indexEdge.srcVertex}] --> tgt[${indexEdge.tgtVertex}]")

    println(s"\t\t<< src vertex id : ${indexEdge.srcVertex.innerId}, bytes : ${GraphUtil.bytesToHexString(srcIdBytes)}")
    println(s"\t\t<< label with dir : ${indexEdge.labelWithDir}, ${GraphUtil.fromDirection(indexEdge.labelWithDir.dir)}, bytes : ${GraphUtil.bytesToHexString(labelWithDirBytes)}")
    println(s"\t\t<< label index : ${indexEdge.labelIndex.name}, seq : ${indexEdge.labelIndex.seq}")
    println(s"\t\t<< label index seq with inverted bytes : ${GraphUtil.bytesToHexString(labelIndexSeqWithIsInvertedBytes)}")

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    println()
    println(s"\t\t<< [RedisIndexEdgeSerializable:toKeyValues] row key : ${GraphUtil.bytesToHexString(row)}")
    println()
    val tgtIdBytes = VertexId.toTargetVertexId(indexEdge.tgtVertex.id).bytes

    /**
     * Qualifier and value byte array map
     *
     *  * byte field design
     *    [{ qualifier total length - 1 byte } | { # of index property - 1 byte } | -
     *     { series of index property values - sum of length with each property values bytes } | -
     *     { timestamp - 8 bytes } | { target id inner value - length of target id inner value bytes } | -
     *     { operation code byte - 1 byte } -
     *     { series of non-index property values - sum of length with each property values bytes }]
     *
     *  ** !Serialize operation code byte after target id or series of index props bytes
     */
    val timestamp = InnerVal(indexEdge.ts).bytes
    val qualifier =
      (idxPropsMap.get(LabelMeta.toSeq) match {
        case None => Bytes.add(idxPropsBytes, timestamp, tgtIdBytes)
        case Some(vId) => Bytes.add(idxPropsBytes, timestamp)
      }) ++ Array.fill(1)(indexEdge.op)

    val qualifierLen = Array.fill[Byte](1)(qualifier.length.toByte)
    val propsKv = propsToKeyValues(indexEdge.metas.toSeq)

    val value = qualifierLen ++ qualifier ++ propsKv
    val emptyArray = Array.empty[Byte]
    val kv = SKeyValue(emptyArray, row, emptyArray, emptyArray, value, indexEdge.version)

    Seq(kv)
  }
}
