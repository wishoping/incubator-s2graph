package com.kakao.s2graph.core.storage.redis.jedis

import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis._

/**
  * Created by june.kay on 2016. 2. 10..
  */
class JedisTransaction(passClient: Client) extends Transaction(passClient) {

  def evalWithLong(script: Array[Byte], keys: List[Array[Byte]], args: List[Array[Byte]]): Response[java.lang.Long] = {
    val params: Array[Array[Byte]] = (keys ++ args).toArray

    getClient(script).eval(script, Protocol.toByteArray(keys.length), params)

    return getResponse(BuilderFactory.LONG)
  }

}
