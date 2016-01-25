package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.Integrate.IntegrateCommon
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.{Graph, Management}
import com.kakao.s2graph.core.mysqls.Label
import com.typesafe.config.{ConfigValueFactory, ConfigValue, ConfigFactory}
import play.api.libs.json.Json

import collection.JavaConversions._

import scala.concurrent.ExecutionContext

/**
  * Created by june.kay on 2016. 1. 20..
  */
class RedisCrudTest extends IntegrateCommon{
  import TestUtil._

  override def beforeAll = {
    config = ConfigFactory.load()
      .withValue("storage.engine", ConfigValueFactory.fromAnyRef("redis")) // for redis test
      .withValue("storage.redis.instances", ConfigValueFactory.fromIterable(List[String]("localhost"))) // for redis test

    println(s">> Config for storage.engine : ${config.getString("storage.engine")}")
    println(s">> Config for redis.instances : ${config.getStringList("storage.redis.instances").mkString(",")}")

    graph = new Graph(config)(ExecutionContext.Implicits.global)
    management = new Management(graph)
    parser = new RequestParser(graph.config)
    initTestData()
  }

  /**
    * Make Service, Label, Vertex for integrate test
    */
  override def initTestData() = {
    println("[Redis init start]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    Management.deleteService(testServiceName)

    // 1. createService
    val jsValue = Json.parse(createService)
    val (serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm) =
      parser.toServiceElements(jsValue)

    val tryRes =
      management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
    println(s">> Service created : $createService, $tryRes")

    // with only v3 label
    val labelNames = Map(testLabelName2 -> testLabelName2Create)

    for {
      (labelName, create) <- labelNames
    } {
      Management.deleteLabel(labelName)
      Label.findByName(labelName, useCache = false) match {
        case None =>
          val json = Json.parse(create)
          val tryRes = for {
            labelArgs <- parser.toLabelElements(json)
            label <- (management.createLabel _).tupled(labelArgs)
          } yield label

          tryRes.get
        case Some(label) =>
          println(s">> Label already exist: $create, $label")
      }
    }

    val vertexPropsKeys = List("age" -> "int")

    vertexPropsKeys.map { case (key, keyType) =>
      Management.addVertexProp(testServiceName, testColumnName, key, keyType)
    }

    println("[Redis init end]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }


  test("test CRUD") {
    println(">>>>> Test crud")
  }


}
