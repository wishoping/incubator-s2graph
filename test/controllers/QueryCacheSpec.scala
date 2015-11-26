package controllers

//import com.kakao.s2graph.core.models._

import controllers.{SpecCommon, EdgeController}
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

class QueryCacheSpec extends SpecCommon {
  init()

  "cache test" should {
    def queryWithTTL(id: Int, cacheTTL: Long) = Json.parse( s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [[ {
            "label": "${testLabelName}",
            "direction": "out",
            "offset": 0,
            "limit": 10,
            "cacheTTL": ${cacheTTL},
            "scoring": {"weight": 1} }]]
          }""")

    def getEdges(queryJson: JsValue): JsValue = {
      var ret = route(FakeRequest(POST, "/graphs/getEdges").withJsonBody(queryJson)).get
      contentAsJson(ret)
    }

    // init
    running(FakeApplication()) {
      // insert bulk and wait ..
      val bulkEdges: String = Seq(
        Seq("1", "insert", "e", "0", "2", "s2graph_label_test", "{}").mkString("\t"),
        Seq("1", "insert", "e", "1", "2", "s2graph_label_test", "{}").mkString("\t")
      ).mkString("\n")

      val jsResult = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))
      Thread.sleep(asyncFlushInterval)
    }

    "tc1: query with {id: 0, ttl: 2000}" in {
      running(FakeApplication()) {
        val ttl = 2000
        val sleep = 500
        var jsRslt = getEdges(queryWithTTL(0, ttl))
        println(jsRslt)
        var cacheRemain = (jsRslt \\ "cacheRemain").head.as[Int]
        cacheRemain must greaterThan(500)

        // get edges from cache after wait 500ms
        Thread.sleep(sleep)
        jsRslt = getEdges(queryWithTTL(0, ttl))
        println(jsRslt)
        cacheRemain = (jsRslt \\ "cacheRemain").head.as[Int]
        cacheRemain must lessThan(ttl - sleep)
      }
    }

    "tc2: query with {id: 1, ttl: 5000}" in {
      running(FakeApplication()) {
        val ttl = 5000
        var jsRslt = getEdges(queryWithTTL(1, ttl))
        var cacheRemain = (jsRslt \\ "cacheRemain").head
        // before update: is_blocked is false
        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))
        println(jsRslt.toString)
        val bulkEdges = Seq(
          Seq("2", "update", "e", "0", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t"),
          Seq("2", "update", "e", "1", "2", "s2graph_label_test", "{\"is_blocked\": true}").mkString("\t")
        ).mkString("\n")

        // update edges with {is_blocked: true}
        jsRslt = contentAsJson(EdgeController.mutateAndPublish(bulkEdges, withWait = true))

        // prop 'is_blocked' still false, cause queryResult on cache
        jsRslt = getEdges(queryWithTTL(1, ttl))

        println("==========")
        println(jsRslt.toString)

        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(false))
        // after wait 3000ms prop 'is_blocked' is updated to true, cache cleared
        Thread.sleep(ttl)
        jsRslt = getEdges(queryWithTTL(1, ttl))
        (jsRslt \\ "is_blocked").head must equalTo(JsBoolean(true))
      }
    }
  }
}
