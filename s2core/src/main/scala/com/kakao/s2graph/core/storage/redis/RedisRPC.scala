package com.kakao.s2graph.core.storage.redis

/**
 * Created by jojo on 1/22/16.
 */
abstract class RedisRPC(key: Array[Byte])

case class RedisGetRequest(key: Array[Byte]) extends RedisRPC(key)