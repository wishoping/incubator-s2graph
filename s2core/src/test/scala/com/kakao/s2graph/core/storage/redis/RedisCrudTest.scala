package com.kakao.s2graph.core.storage.redis

import com.kakao.s2graph.core.Integrate.IntegrateCommon
import com.kakao.s2graph.core.mysqls.Label
import com.kakao.s2graph.core.rest.RequestParser
import com.kakao.s2graph.core.{Graph, Management}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

/**
  * Created by june.kay on 2016. 1. 20..
  */
class RedisCrudTest extends IntegrateCommon with BeforeAndAfterEach {
  import TestUtil._

  val insert = "insert"
  val e = "e"

  override def beforeAll = {
    config = ConfigFactory.load()
      .withValue("storage.engine", ConfigValueFactory.fromAnyRef("redis")) // for redis test
      .withValue("storage.redis.instances", ConfigValueFactory.fromIterable(List[String]("localhost"))) // for redis test

    println(s">> Config for storage.engine : ${config.getString("storage.engine")}")
    println(s">> Config for redis.instances : ${config.getStringList("storage.redis.instances").mkString(",")}")

    graph = new Graph(config)(ExecutionContext.Implicits.global)
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
      Management.createService(serviceName, cluster, tableName, preSplitSize, ttl, compressionAlgorithm)
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
            label <- (Management.createLabel _).tupled(labelArgs)
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


  test("test insert/check edges") {
    insertEdgesSync(
      toEdge(1, insert, e, 1, 1000, testLabelName2),
      toEdge(1, insert, e, 2, 2000, testLabelName2)
    )
    def queryCheckEdges(fromId: Int, toId: Int): JsValue = {
      Json.arr(
        Json.obj(
          "label" -> testLabelName2,
          "direction" -> "out",
          "from" -> fromId,
          "to" -> toId
        )
      )
    }

    var result = checkEdgesSync(queryCheckEdges(1, 1000))
    (result \ "size").toString should be ("1")  // edge 1 -> 1000 should be present

    result = checkEdgesSync(queryCheckEdges(2, 2000))
    (result \ "size").toString should be ("1")  // edge 2 -> 2000 should be present
  }


}
