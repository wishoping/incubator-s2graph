package com.kakao.s2graph.core.Integrate

import com.kakao.s2graph.core.V2Test
import org.scalatest.BeforeAndAfterEach
import play.api.libs.json._

class QueryTest extends IntegrateCommon with BeforeAndAfterEach {

  import TestUtil._

  val insert = "insert"
  val e = "e"
  val weight = "weight"
  val is_hidden = "is_hidden"

  test("interval", V2Test) {
    def queryWithInterval(id: Int, index: String, prop: String, fromVal: Int, toVal: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$testLabelNameV2",
              "index": "$index",
              "interval": {
                  "from": [ { "$prop": $fromVal } ],
                  "to": [ { "$prop": $toVal } ]
              }
            }
          ]]
        }
        """)

    var edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 1001)) // test interval on timestamp index
    (edges \ "size").toString should be("1")

    edges = getEdgesSync(queryWithInterval(0, index2, "_timestamp", 1000, 2000)) // test interval on timestamp index
    (edges \ "size").toString should be("2")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 11)) // test interval on weight index
    (edges \ "size").toString should be("1")

    edges = getEdgesSync(queryWithInterval(2, index1, "weight", 10, 20)) // test interval on weight index
    (edges \ "size").toString should be("2")
  }

  test("get edge with where condition", V2Test) {
    def queryWhere(id: Int, where: String) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameV2}",
              "direction": "out",
              "offset": 0,
              "limit": 100,
              "where": "${where}"
            }
          ]]
        }""")

    var result = getEdgesSync(queryWhere(0, "is_hidden=false and _from in (-1, 0)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(queryWhere(0, "is_hidden=true and _to in (1)"))
    (result \ "results").as[List[JsValue]].size should be(1)

    result = getEdgesSync(queryWhere(0, "_from=0"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryWhere(2, "_from=2 or weight in (-1)"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryWhere(2, "_from=2 and weight in (10, 20)"))
    (result \ "results").as[List[JsValue]].size should be(2)
  }

  test("get edge exclude", V2Test) {
    def queryExclude(id: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameV2}",
              "direction": "out",
              "offset": 0,
              "limit": 2
            },
            {
              "label": "${testLabelNameV2}",
              "direction": "in",
              "offset": 0,
              "limit": 2,
              "exclude": true
            }
          ]]
        }""")

    val result = getEdgesSync(queryExclude(0))
    (result \ "results").as[List[JsValue]].size should be(1)
  }

  test("get edge groupBy property", V2Test) {
    def queryGroupBy(id: Int, props: Seq[String]): JsValue = {
      Json.obj(
        "groupBy" -> props,
        "srcVertices" -> Json.arr(
          Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
        ),
        "steps" -> Json.arr(
          Json.obj(
            "step" -> Json.arr(
              Json.obj(
                "label" -> testLabelNameV2
              )
            )
          )
        )
      )
    }

    val result = getEdgesSync(queryGroupBy(0, Seq("weight")))
    (result \ "size").as[Int] should be(2)
    val weights = (result \\ "groupBy").map { js =>
      (js \ "weight").as[Int]
    }
    weights should contain(30)
    weights should contain(40)

    weights should not contain (10)
  }

  test("edge transform", V2Test) {
    def queryTransform(id: Int, transforms: String) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameV2}",
              "direction": "out",
              "offset": 0,
              "transform": $transforms
            }
          ]]
        }""")

    var result = getEdgesSync(queryTransform(0, "[[\"_to\"]]"))
    (result \ "results").as[List[JsValue]].size should be(2)

    result = getEdgesSync(queryTransform(0, "[[\"weight\"]]"))
    (result \\ "to").map(_.toString).sorted should be((result \\ "weight").map(_.toString).sorted)

    result = getEdgesSync(queryTransform(0, "[[\"_from\"]]"))
    val results = (result \ "results").as[JsValue]
    (result \\ "to").map(_.toString).sorted should be((results \\ "from").map(_.toString).sorted)
  }

  test("index", V2Test) {
    def queryIndex(ids: Seq[Int], indexName: String) = {
      val $from = Json.arr(
        Json.obj("serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "ids" -> ids))

      val $step = Json.arr(Json.obj("label" -> testLabelNameV2, "index" -> indexName))
      val $steps = Json.arr(Json.obj("step" -> $step))

      val js = Json.obj("withScore" -> false, "srcVertices" -> $from, "steps" -> $steps)
      js
    }

    // weight order
    var result = getEdgesSync(queryIndex(Seq(0), "idx_1"))
    ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(40))

    // timestamp order
    result = getEdgesSync(queryIndex(Seq(0), "idx_2"))
    ((result \ "results").as[List[JsValue]].head \\ "weight").head should be(JsNumber(30))
  }

  //    "checkEdges" in {
  //      running(FakeApplication()) {
  //        val json = Json.parse( s"""
  //         [{"from": 0, "to": 1, "label": "$testLabelName"},
  //          {"from": 0, "to": 2, "label": "$testLabelName"}]
  //        """)
  //
  //        def checkEdges(queryJson: JsValue): JsValue = {
  //          val ret = route(FakeRequest(POST, "/graphs/checkEdges").withJsonBody(queryJson)).get
  //          contentAsJson(ret)
  //        }
  //
  //        val res = checkEdges(json)
  //        val typeRes = res.isInstanceOf[JsArray]
  //        typeRes must equalTo(true)
  //
  //        val fst = res.as[Seq[JsValue]].head \ "to"
  //        fst.as[Int] must equalTo(1)
  //
  //        val snd = res.as[Seq[JsValue]].last \ "to"
  //        snd.as[Int] must equalTo(2)
  //      }
  //    }

  test("return tree", V2Test) {
    def queryParents(id: Long) = Json.parse(
      s"""
        {
          "returnTree": true,
          "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
          [ {
              "label": "$testLabelNameV2",
              "direction": "out",
              "offset": 0,
              "limit": 2
            }
          ],[{
              "label": "$testLabelNameV2",
              "direction": "in",
              "offset": 0,
              "limit": -1
            }
          ]]
        }""".stripMargin)

    val src = 100
    val tgt = 200

    insertEdgesSync(toEdge(1001, "insert", "e", src, tgt, testLabelNameV2))

    val result = TestUtil.getEdgesSync(queryParents(src))
    val parents = (result \ "results").as[Seq[JsValue]]
    val ret = parents.forall {
      edge => (edge \ "parents").as[Seq[JsValue]].size == 1
    }

    ret should be(true)
  }



  test("pagination and _to", V2Test) {
    def querySingleWithTo(id: Int, offset: Int = 0, limit: Int = 100, to: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "${testServiceName}",
            "columnName": "${testColumnName}",
            "id": ${id}
           }],
          "steps": [
          [ {
              "label": "${testLabelNameV2}",
              "direction": "out",
              "offset": $offset,
              "limit": $limit,
              "_to": $to
            }
          ]]
        }
        """)

    val src = System.currentTimeMillis().toInt

    val bulkEdges = Seq(
      toEdge(1001, insert, e, src, 1, testLabelNameV2, Json.obj(weight -> 10, is_hidden -> true)),
      toEdge(2002, insert, e, src, 2, testLabelNameV2, Json.obj(weight -> 20, is_hidden -> false)),
      toEdge(3003, insert, e, src, 3, testLabelNameV2, Json.obj(weight -> 30)),
      toEdge(4004, insert, e, src, 4, testLabelNameV2, Json.obj(weight -> 40))
    )
    insertEdgesSync(bulkEdges: _*)

    var result = getEdgesSync(querySingle(src, offset = 0, limit = 2))
    var edges = (result \ "results").as[List[JsValue]]

    edges.size should be(2)
    (edges(0) \ "to").as[Long] should be(4)
    (edges(1) \ "to").as[Long] should be(3)

    result = getEdgesSync(querySingle(src, offset = 1, limit = 2))

    edges = (result \ "results").as[List[JsValue]]
    edges.size should be(2)
    (edges(0) \ "to").as[Long] should be(3)
    (edges(1) \ "to").as[Long] should be(2)

    result = getEdgesSync(querySingleWithTo(src, offset = 0, limit = -1, to = 1))
    edges = (result \ "results").as[List[JsValue]]
    edges.size should be(1)
  }

  test("order by", V2Test) {
    def queryScore(id: Int, scoring: Map[String, Int]): JsValue = Json.obj(
      "srcVertices" -> Json.arr(
        Json.obj(
          "serviceName" -> testServiceName,
          "columnName" -> testColumnName,
          "id" -> id
        )
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> testLabelNameV2,
              "scoring" -> scoring
            )
          )
        )
      )
    )
    def queryOrderBy(id: Int, scoring: Map[String, Int], props: Seq[Map[String, String]]): JsValue = Json.obj(
      "orderBy" -> props,
      "srcVertices" -> Json.arr(
        Json.obj("serviceName" -> testServiceName, "columnName" -> testColumnName, "id" -> id)
      ),
      "steps" -> Json.arr(
        Json.obj(
          "step" -> Json.arr(
            Json.obj(
              "label" -> testLabelNameV2,
              "scoring" -> scoring
            )
          )
        )
      )
    )

    val bulkEdges = Seq(
      toEdge(1001, insert, e, 0, 1, testLabelNameV2, Json.obj(weight -> 10, is_hidden -> true)),
      toEdge(2002, insert, e, 0, 2, testLabelNameV2, Json.obj(weight -> 20, is_hidden -> false)),
      toEdge(3003, insert, e, 2, 0, testLabelNameV2, Json.obj(weight -> 30)),
      toEdge(4004, insert, e, 2, 1, testLabelNameV2, Json.obj(weight -> 40))
    )

    insertEdgesSync(bulkEdges: _*)

    // get edges
    val edges = getEdgesSync(queryScore(0, Map("weight" -> 1)))
    val orderByScore = getEdgesSync(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "DESC", "timestamp" -> "DESC"))))
    val ascOrderByScore = getEdgesSync(queryOrderBy(0, Map("weight" -> 1), Seq(Map("score" -> "ASC", "timestamp" -> "DESC"))))

    val edgesTo = edges \ "results" \\ "to"
    val orderByTo = orderByScore \ "results" \\ "to"
    val ascOrderByTo = ascOrderByScore \ "results" \\ "to"

    edgesTo should be(Seq(JsNumber(2), JsNumber(1)))
    edgesTo should be(orderByTo)
    ascOrderByTo should be(Seq(JsNumber(1), JsNumber(2)))
    edgesTo.reverse should be(ascOrderByTo)
  }

  test("query with sampling", V2Test) {
    def queryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelNameV2",
                "direction": "out",
                "offset": 0,
                "limit": 100,
                "sample": $sample
                }]
            }
          ]
        }""")

    def twoStepQueryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelNameV2",
                "direction": "out",
                "offset": 0,
                "limit": 100,
                "sample": $sample
                }]
            },
            {
               "step": [{
                 "label": "$testLabelNameV2",
                 "direction": "out",
                 "offset": 0,
                 "limit": 100,
                 "sample": $sample
               }]
            }
          ]
        }""")

    def twoQueryWithSampling(id: Int, sample: Int) = Json.parse(
      s"""
        { "srcVertices": [
          { "serviceName": "$testServiceName",
            "columnName": "$testColumnName",
            "id": $id
           }],
          "steps": [
            {
              "step": [{
                "label": "$testLabelNameV2",
                "direction": "out",
                "offset": 0,
                "limit": 50,
                "sample": $sample
              },
              {
                "label": "$testLabelNameV3",
                "direction": "out",
                "offset": 0,
                "limit": 50
              }]
            }
          ]
        }""")

    val sampleSize = 2
    val ts = "1442985659166"
    val testId = 22

    val bulkEdges = Seq(
      toEdge(ts, insert, e, testId, 122, testLabelNameV2),
      toEdge(ts, insert, e, testId, 222, testLabelNameV2),
      toEdge(ts, insert, e, testId, 322, testLabelNameV2),

      toEdge(ts, insert, e, testId, 922, testLabelNameV3),
      toEdge(ts, insert, e, testId, 222, testLabelNameV3),
      toEdge(ts, insert, e, testId, 322, testLabelNameV3),

      toEdge(ts, insert, e, 122, 1122, testLabelNameV2),
      toEdge(ts, insert, e, 122, 1222, testLabelNameV2),
      toEdge(ts, insert, e, 122, 1322, testLabelNameV2),
      toEdge(ts, insert, e, 222, 2122, testLabelNameV2),
      toEdge(ts, insert, e, 222, 2222, testLabelNameV2),
      toEdge(ts, insert, e, 222, 2322, testLabelNameV2),
      toEdge(ts, insert, e, 322, 3122, testLabelNameV2),
      toEdge(ts, insert, e, 322, 3222, testLabelNameV2),
      toEdge(ts, insert, e, 322, 3322, testLabelNameV2)
    )

    insertEdgesSync(bulkEdges: _*)

    val result1 = getEdgesSync(queryWithSampling(testId, sampleSize))
    (result1 \ "results").as[List[JsValue]].size should be(math.min(sampleSize, bulkEdges.size))

    val result2 = getEdgesSync(twoStepQueryWithSampling(testId, sampleSize))
    (result2 \ "results").as[List[JsValue]].size should be(math.min(sampleSize * sampleSize, bulkEdges.size * bulkEdges.size))

    val result3 = getEdgesSync(twoQueryWithSampling(testId, sampleSize))
    (result3 \ "results").as[List[JsValue]].size should be(sampleSize + 3) // edges in testLabelName2 = 3
  }

  def querySingle(id: Int, offset: Int = 0, limit: Int = 100) = Json.parse(
    s"""
          { "srcVertices": [
            { "serviceName": "$testServiceName",
              "columnName": "$testColumnName",
              "id": $id
             }],
            "steps": [
            [ {
                "label": "$testLabelNameV2",
                "direction": "out",
                "offset": $offset,
                "limit": $limit
              }
            ]]
          }
          """)

  // called by each test, each
  override def beforeEach = initTestData()

  // called by start test, once
  override def initTestData(): Unit = {
    super.initTestData()

    insertEdgesSync(
      toEdge(1000, insert, e, 0, 1, testLabelNameV2, Json.obj(weight -> 40, is_hidden -> true)),
      toEdge(2000, insert, e, 0, 2, testLabelNameV2, Json.obj(weight -> 30, is_hidden -> false)),
      toEdge(3000, insert, e, 2, 0, testLabelNameV2, Json.obj(weight -> 20)),
      toEdge(4000, insert, e, 2, 1, testLabelNameV2, Json.obj(weight -> 10)),
      toEdge(3000, insert, e, 10, 20, testLabelNameV2, Json.obj(weight -> 20)),
      toEdge(4000, insert, e, 20, 20, testLabelNameV2, Json.obj(weight -> 10)),
      toEdge(1, insert, e, -1, 1000, testLabelNameV2),
      toEdge(1, insert, e, -1, 2000, testLabelNameV2),
      toEdge(1, insert, e, -1, 3000, testLabelNameV2),
      toEdge(1, insert, e, 1000, 10000, testLabelNameV2),
      toEdge(1, insert, e, 1000, 11000, testLabelNameV2),
      toEdge(1, insert, e, 2000, 11000, testLabelNameV2),
      toEdge(1, insert, e, 2000, 12000, testLabelNameV2),
      toEdge(1, insert, e, 3000, 12000, testLabelNameV2),
      toEdge(1, insert, e, 3000, 13000, testLabelNameV2),
      toEdge(1, insert, e, 10000, 100000, testLabelNameV2),
      toEdge(2, insert, e, 11000, 200000, testLabelNameV2),
      toEdge(3, insert, e, 12000, 300000, testLabelNameV2)
    )
  }
}
