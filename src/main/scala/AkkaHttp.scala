import java.util.concurrent.Executors

//import akka.actor.ActorSystem
//import akka.event.{Logging, LoggingAdapter}
//import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes}
//import akka.http.scaladsl.server.Directives._
//import akka.stream.{ActorMaterializer, Materializer}
import com.kakao.s2graph.core.GraphExceptions.{BadQueryException, ModelNotFoundException}
import com.kakao.s2graph.core._

//import com.kakao.s2graph.core.OrderingUtil._
import com.kakao.s2graph.core.OrderingUtil._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.parsers.WhereParser
import com.kakao.s2graph.core.types._
import com.typesafe.config.ConfigFactory
import play.api.libs.json._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util._

trait RequestParser extends JSONParser {

  import Management.JsonModel._

  val hardLimit = 10000
  val defaultLimit = 100

  lazy val defaultCluster = "localhost"
  // to be removed
  lazy val defaultCompressionAlgorithm = "lz4" // to be removed

  private def extractScoring(labelId: Int, value: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](value, "scoring")
    } yield {
      for {
        (k, v) <- js.fields
        labelOrderType <- LabelMeta.findByName(labelId, k)
      } yield {
        val value = v match {
          case n: JsNumber => n.as[Double]
          case _ => throw new Exception("scoring weight should be double.")
        }
        (labelOrderType.seq, value)
      }
    }
    ret
  }

  def extractInterval(label: Label, jsValue: JsValue) = {
    def extractKv(js: JsValue) = js match {
      case JsObject(obj) => obj
      case JsArray(arr) => arr.flatMap {
        case JsObject(obj) => obj
        case _ => throw new RuntimeException(s"cannot support json type $js")
      }
      case _ => throw new RuntimeException(s"cannot support json type: $js")
    }

    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "interval")
      fromJs <- (js \ "from").asOpt[JsValue]
      toJs <- (js \ "to").asOpt[JsValue]
    } yield {
      val from = Management.toProps(label, extractKv(fromJs))
      val to = Management.toProps(label, extractKv(toJs))
      (from, to)
    }

    ret
  }

  def extractDuration(label: Label, jsValue: JsValue) = {
    for {
      js <- parse[Option[JsObject]](jsValue, "duration")
    } yield {
      val minTs = parse[Option[Long]](js, "from").getOrElse(Long.MaxValue)
      val maxTs = parse[Option[Long]](js, "to").getOrElse(Long.MinValue)

      if (minTs > maxTs) {
        throw new RuntimeException("Duration error. Timestamp of From cannot be larger than To.")
      }
      (minTs, maxTs)
    }
  }

  def extractHas(label: Label, jsValue: JsValue) = {
    val ret = for {
      js <- parse[Option[JsObject]](jsValue, "has")
    } yield {
      for {
        (k, v) <- js.fields
        labelMeta <- LabelMeta.findByName(label.id.get, k)
        value <- jsValueToInnerVal(v, labelMeta.dataType, label.schemaVersion)
      } yield {
        labelMeta.seq -> value
      }
    }
    ret.map(_.toMap).getOrElse(Map.empty[Byte, InnerValLike])
  }

  def extractWhere(labelMap: Map[String, Label], jsValue: JsValue) = {
    (jsValue \ "where").asOpt[String] match {
      case None => Success(WhereParser.success)
      case Some(where) =>
        WhereParser(labelMap).parse(where) match {
          case s@Success(_) => s
          case Failure(ex) => throw BadQueryException(ex.getMessage, ex)
        }
    }
  }

  def toVertices(labelName: String, direction: String, ids: Seq[JsValue]): Seq[Vertex] = {
    val vertices = for {
      label <- Label.findByName(labelName).toSeq
      serviceColumn = if (direction == "out") label.srcColumn else label.tgtColumn
      id <- ids
      innerId <- jsValueToInnerVal(id, serviceColumn.columnType, label.schemaVersion)
    } yield {
      Vertex(SourceVertexId(serviceColumn.id.get, innerId), System.currentTimeMillis())
    }
    vertices.toSeq
  }

  def toQuery(jsValue: JsValue, isEdgeQuery: Boolean = true): Query = {
    try {
      val vertices =
        (for {
          value <- parse[List[JsValue]](jsValue, "srcVertices")
          serviceName = parse[String](value, "serviceName")
          column = parse[String](value, "columnName")
        } yield {
          val service = Service.findByName(serviceName).getOrElse(throw BadQueryException("service not found"))
          val col = ServiceColumn.find(service.id.get, column).getOrElse(throw BadQueryException("bad column name"))
          val (idOpt, idsOpt) = ((value \ "id").asOpt[JsValue], (value \ "ids").asOpt[List[JsValue]])
          for {
            idVal <- idOpt ++ idsOpt.toSeq.flatten

            /** bug, need to use labels schemaVersion  */
            innerVal <- jsValueToInnerVal(idVal, col.columnType, col.schemaVersion)
          } yield {
            Vertex(SourceVertexId(col.id.get, innerVal), System.currentTimeMillis())
          }
        }).flatten

      if (vertices.isEmpty) throw BadQueryException("srcVertices`s id is empty")

      val filterOutFields = (jsValue \ "filterOutFields").asOpt[List[String]].getOrElse(List(LabelMeta.to.name))
      val filterOutQuery = (jsValue \ "filterOut").asOpt[JsValue].map { v => toQuery(v) }.map { q => q.copy(filterOutFields = filterOutFields) }
      val steps = parse[Vector[JsValue]](jsValue, "steps")
      val removeCycle = (jsValue \ "removeCycle").asOpt[Boolean].getOrElse(true)
      val selectColumns = (jsValue \ "select").asOpt[List[String]].getOrElse(List.empty)
      val groupByColumns = (jsValue \ "groupBy").asOpt[List[String]].getOrElse(List.empty)
      val orderByColumns: List[(String, Boolean)] = (jsValue \ "orderBy").asOpt[List[JsObject]].map { jsLs =>
        for {
          js <- jsLs
          (column, orderJs) <- js.fields
        } yield {
          val ascending = orderJs.as[String].toUpperCase match {
            case "ASC" => true
            case "DESC" => false
          }
          column -> ascending
        }
      }.getOrElse(List("score" -> false, "timestamp" -> false))
      val withScore = (jsValue \ "withScore").asOpt[Boolean].getOrElse(true)
      val returnTree = (jsValue \ "returnTree").asOpt[Boolean].getOrElse(false)

      // TODO: throw exception, when label dosn't exist
      val labelMap = (for {
        js <- jsValue \\ "label"
        labelName <- js.asOpt[String]
        label <- Label.findByName(labelName)
      } yield (labelName, label)).toMap

      val querySteps =
        steps.zipWithIndex.map { case (step, stepIdx) =>
          val labelWeights = step match {
            case obj: JsObject =>
              val converted = for {
                (k, v) <- (obj \ "weights").asOpt[JsObject].getOrElse(Json.obj()).fields
                l <- Label.findByName(k)
              } yield {
                l.id.get -> v.toString().toDouble
              }
              converted.toMap
            case _ => Map.empty[Int, Double]
          }
          val queryParamJsVals = step match {
            case arr: JsArray => arr.as[List[JsValue]]
            case obj: JsObject => (obj \ "step").as[List[JsValue]]
            case _ => List.empty[JsValue]
          }
          val nextStepScoreThreshold = step match {
            case obj: JsObject => (obj \ "nextStepThreshold").asOpt[Double].getOrElse(QueryParam.DefaultThreshold)
            case _ => QueryParam.DefaultThreshold
          }
          val nextStepLimit = step match {
            case obj: JsObject => (obj \ "nextStepLimit").asOpt[Int].getOrElse(-1)
            case _ => -1
          }
          val cacheTTL = step match {
            case obj: JsObject => (obj \ "cacheTTL").asOpt[Int].getOrElse(-1)
            case _ => -1
          }
          val queryParams =
            for {
              labelGroup <- queryParamJsVals
              queryParam <- parseQueryParam(labelMap, labelGroup)
            } yield {
              val (_, columnName) =
                if (queryParam.labelWithDir.dir == GraphUtil.directions("out")) {
                  (queryParam.label.srcService.serviceName, queryParam.label.srcColumnName)
                } else {
                  (queryParam.label.tgtService.serviceName, queryParam.label.tgtColumnName)
                }
              //FIXME:
              if (stepIdx == 0 && vertices.nonEmpty && !vertices.exists(v => v.serviceColumn.columnName == columnName)) {
                throw BadQueryException("srcVertices contains incompatiable serviceName or columnName with first step.")
              }

              queryParam
            }
          Step(queryParams.toList, labelWeights = labelWeights,
            //            scoreThreshold = stepThreshold,
            nextStepScoreThreshold = nextStepScoreThreshold,
            nextStepLimit = nextStepLimit,
            cacheTTL = cacheTTL)

        }

      val ret = Query(
        vertices,
        querySteps,
        removeCycle = removeCycle,
        selectColumns = selectColumns,
        groupByColumns = groupByColumns,
        orderByColumns = orderByColumns,
        filterOutQuery = filterOutQuery,
        filterOutFields = filterOutFields,
        withScore = withScore,
        returnTree = returnTree
      )
      //      logger.debug(ret.toString)
      ret
    } catch {
      case e: BadQueryException =>
        throw e
      case e: ModelNotFoundException =>
        throw BadQueryException(e.getMessage, e)
      case e: Exception =>
        throw BadQueryException(s"$jsValue, $e", e)
    }
  }

  private def parseQueryParam(labelMap: Map[String, Label], labelGroup: JsValue): Option[QueryParam] = {
    for {
      labelName <- parse[Option[String]](labelGroup, "label")
    } yield {
      val label = Label.findByName(labelName).getOrElse(throw BadQueryException(s"$labelName not found"))
      val direction = parse[Option[String]](labelGroup, "direction").map(GraphUtil.toDirection(_)).getOrElse(0)
      val limit = {
        parse[Option[Int]](labelGroup, "limit") match {
          case None => defaultLimit
          case Some(l) if l < 0 => Int.MaxValue
          case Some(l) if l >= 0 =>
            val default = hardLimit
            Math.min(l, default)
        }
      }
      val offset = parse[Option[Int]](labelGroup, "offset").getOrElse(0)
      val interval = extractInterval(label, labelGroup)
      val duration = extractDuration(label, labelGroup)
      val scoring = extractScoring(label.id.get, labelGroup).getOrElse(List.empty[(Byte, Double)]).toList
      val exclude = parse[Option[Boolean]](labelGroup, "exclude").getOrElse(false)
      val include = parse[Option[Boolean]](labelGroup, "include").getOrElse(false)
      val hasFilter = extractHas(label, labelGroup)
      val labelWithDir = LabelWithDirection(label.id.get, direction)
      val indexNameOpt = (labelGroup \ "index").asOpt[String]
      val indexSeq = indexNameOpt match {
        case None => label.indexSeqsMap.get(scoring.map(kv => kv._1)).map(_.seq).getOrElse(LabelIndex.DefaultSeq)
        case Some(indexName) => label.indexNameMap.get(indexName).map(_.seq).getOrElse(throw new RuntimeException("cannot find index"))
      }
      val where = extractWhere(labelMap, labelGroup)
      val includeDegree = (labelGroup \ "includeDegree").asOpt[Boolean].getOrElse(true)
      val rpcTimeout = (labelGroup \ "rpcTimeout").asOpt[Int].getOrElse(1000)
      val maxAttempt = (labelGroup \ "maxAttempt").asOpt[Int].getOrElse(100)
      val tgtVertexInnerIdOpt = (labelGroup \ "_to").asOpt[JsValue].flatMap { jsVal =>
        jsValueToInnerVal(jsVal, label.tgtColumnWithDir(direction).columnType, label.schemaVersion)
      }
      val cacheTTL = (labelGroup \ "cacheTTL").asOpt[Long].getOrElse(-1L)
      val timeDecayFactor = (labelGroup \ "timeDecay").asOpt[JsObject].map { jsVal =>
        val initial = (jsVal \ "initial").asOpt[Double].getOrElse(1.0)
        val decayRate = (jsVal \ "decayRate").asOpt[Double].getOrElse(0.1)
        if (decayRate >= 1.0 || decayRate <= 0.0) throw new BadQueryException("decay rate should be 0.0 ~ 1.0")
        val timeUnit = (jsVal \ "timeUnit").asOpt[Double].getOrElse(60 * 60 * 24.0)
        TimeDecay(initial, decayRate, timeUnit)
      }
      val threshold = (labelGroup \ "threshold").asOpt[Double].getOrElse(QueryParam.DefaultThreshold)
      // TODO: refactor this. dirty
      val duplicate = parse[Option[String]](labelGroup, "duplicate").map(s => Query.DuplicatePolicy(s))

      val outputField = (labelGroup \ "outputField").asOpt[String].map(s => Json.arr(Json.arr(s)))
      val transformer = if (outputField.isDefined) outputField else (labelGroup \ "transform").asOpt[JsValue]
      val scorePropagateOp = (labelGroup \ "scorePropagateOp").asOpt[String].getOrElse("multiply")
      val sample = (labelGroup \ "sample").asOpt[Int].getOrElse(-1)

      // FIXME: Order of command matter
      QueryParam(labelWithDir)
        .sample(sample)
        .limit(offset, limit)
        .rank(RankParam(label.id.get, scoring))
        .exclude(exclude)
        .include(include)
        .interval(interval)
        .duration(duration)
        .has(hasFilter)
        .labelOrderSeq(indexSeq)
        .where(where)
        .duplicatePolicy(duplicate)
        .includeDegree(includeDegree)
        .rpcTimeout(rpcTimeout)
        .maxAttempt(maxAttempt)
        .tgtVertexInnerIdOpt(tgtVertexInnerIdOpt)
        .cacheTTLInMillis(cacheTTL)
        .timeDecay(timeDecayFactor)
        .threshold(threshold)
        .transformer(transformer)
        .scorePropagateOp(scorePropagateOp)
    }
  }

  private def parse[R](js: JsValue, key: String)(implicit read: Reads[R]): R = {
    (js \ key).validate[R]
      .fold(
        errors => {
          val msg = (JsError.toFlatJson(errors) \ "obj").as[List[JsValue]].map(x => x \ "msg")
          val e = Json.obj("args" -> key, "error" -> msg)
          throw new GraphExceptions.JsonParseException(Json.obj("error" -> key).toString)
        },
        r => {
          r
        })
  }

  def toJsValues(jsValue: JsValue): List[JsValue] = {
    jsValue match {
      case obj: JsObject => List(obj)
      case arr: JsArray => arr.as[List[JsValue]]
      case _ => List.empty[JsValue]
    }

  }

  def toEdges(jsValue: JsValue, operation: String): List[Edge] = {
    toJsValues(jsValue).map(toEdge(_, operation))
  }

  def toEdge(jsValue: JsValue, operation: String) = {

    val srcId = parse[JsValue](jsValue, "from") match {
      case s: JsString => s.as[String]
      case o@_ => s"${o}"
    }
    val tgtId = parse[JsValue](jsValue, "to") match {
      case s: JsString => s.as[String]
      case o@_ => s"${o}"
    }
    val label = parse[String](jsValue, "label")
    val timestamp = parse[Long](jsValue, "timestamp")
    val direction = parse[Option[String]](jsValue, "direction").getOrElse("")
    val props = (jsValue \ "props").asOpt[JsValue].getOrElse("{}")
    Management.toEdge(timestamp, operation, srcId, tgtId, label, direction, props.toString)

  }

  def toVertices(jsValue: JsValue, operation: String, serviceName: Option[String] = None, columnName: Option[String] = None) = {
    toJsValues(jsValue).map(toVertex(_, operation, serviceName, columnName))
  }

  def toVertex(jsValue: JsValue, operation: String, serviceName: Option[String] = None, columnName: Option[String] = None): Vertex = {
    val id = parse[JsValue](jsValue, "id")
    val ts = parse[Option[Long]](jsValue, "timestamp").getOrElse(System.currentTimeMillis())
    val sName = if (serviceName.isEmpty) parse[String](jsValue, "serviceName") else serviceName.get
    val cName = if (columnName.isEmpty) parse[String](jsValue, "columnName") else columnName.get
    val props = (jsValue \ "props").asOpt[JsObject].getOrElse(Json.obj())
    Management.toVertex(ts, operation, id.toString, sName, cName, props.toString)
  }

  def toPropElements(jsObj: JsValue) = Try {
    val propName = (jsObj \ "name").as[String]
    val dataType = InnerVal.toInnerDataType((jsObj \ "dataType").as[String])
    val defaultValue = (jsObj \ "defaultValue").as[JsValue] match {
      case JsString(s) => s
      case _@js => js.toString
    }
    Prop(propName, defaultValue, dataType)
  }

  def toPropsElements(jsValue: JsValue): Seq[Prop] = for {
    jsObj <- jsValue.asOpt[Seq[JsValue]].getOrElse(Nil)
  } yield {
    val propName = (jsObj \ "name").as[String]
    val dataType = InnerVal.toInnerDataType((jsObj \ "dataType").as[String])
    val defaultValue = (jsObj \ "defaultValue").as[JsValue] match {
      case JsString(s) => s
      case _@js => js.toString
    }
    Prop(propName, defaultValue, dataType)
  }

  def toIndicesElements(jsValue: JsValue): Seq[Index] = for {
    jsObj <- jsValue.as[Seq[JsValue]]
    indexName = (jsObj \ "name").as[String]
    propNames = (jsObj \ "propNames").as[Seq[String]]
  } yield Index(indexName, propNames)

  def toLabelElements(jsValue: JsValue) = Try {
    val labelName = parse[String](jsValue, "label")
    val srcServiceName = parse[String](jsValue, "srcServiceName")
    val tgtServiceName = parse[String](jsValue, "tgtServiceName")
    val srcColumnName = parse[String](jsValue, "srcColumnName")
    val tgtColumnName = parse[String](jsValue, "tgtColumnName")
    val srcColumnType = parse[String](jsValue, "srcColumnType")
    val tgtColumnType = parse[String](jsValue, "tgtColumnType")
    val serviceName = (jsValue \ "serviceName").asOpt[String].getOrElse(tgtServiceName)
    val isDirected = (jsValue \ "isDirected").asOpt[Boolean].getOrElse(true)

    val allProps = toPropsElements(jsValue \ "props")
    val indices = toIndicesElements(jsValue \ "indices")

    val consistencyLevel = (jsValue \ "consistencyLevel").asOpt[String].getOrElse("weak")

    // expect new label don`t provide hTableName
    val hTableName = (jsValue \ "hTableName").asOpt[String]
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val schemaVersion = (jsValue \ "schemaVersion").asOpt[String].getOrElse(HBaseType.DEFAULT_VERSION)
    val isAsync = (jsValue \ "isAsync").asOpt[Boolean].getOrElse(false)
    val compressionAlgorithm = (jsValue \ "compressionAlgorithm").asOpt[String].getOrElse(defaultCompressionAlgorithm)

    (labelName, srcServiceName, srcColumnName, srcColumnType,
      tgtServiceName, tgtColumnName, tgtColumnType, isDirected, serviceName,
      indices, allProps, consistencyLevel, hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm)
  }

  def toIndexElements(jsValue: JsValue) = Try {
    val labelName = parse[String](jsValue, "label")
    val indices = toIndicesElements(jsValue \ "indices")
    (labelName, indices)
  }

  def toServiceElements(jsValue: JsValue) = {
    val serviceName = parse[String](jsValue, "serviceName")
    val cluster = (jsValue \ "cluster").asOpt[String].getOrElse(defaultCluster)
    val hTableName = (jsValue \ "hTableName").asOpt[String].getOrElse(s"${serviceName}-alpha")
    val preSplitSize = (jsValue \ "preSplitSize").asOpt[Int].getOrElse(1)
    val hTableTTL = (jsValue \ "hTableTTL").asOpt[Int]
    val compressionAlgorithm = (jsValue \ "compressionAlgorithm").asOpt[String].getOrElse(defaultCompressionAlgorithm)
    (serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)
  }

  def toServiceColumnElements(jsValue: JsValue) = Try {
    val serviceName = parse[String](jsValue, "serviceName")
    val columnName = parse[String](jsValue, "columnName")
    val columnType = parse[String](jsValue, "columnType")
    val props = toPropsElements(jsValue \ "props")
    (serviceName, columnName, columnType, props)
  }
}


object PostProcess extends JSONParser {
  /**
    * Result Entity score field name
    */
  val SCORE_FIELD_NAME = "scoreSum"
  val timeoutResults = Json.obj("size" -> 0, "results" -> Json.arr(), "isTimeout" -> true)
  val reservedColumns = Set("cacheRemain", "from", "to", "label", "direction", "_timestamp", "timestamp", "score", "props")

  def groupEdgeResult(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap
    //    filterNot {case (edge, score) => edge.props.contains(LabelMeta.degreeSeq)}
    val groupedEdgesWithRank = (for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryRequest.queryParam, queryRequest.query.filterOutFields))
    } yield {
      (queryRequest.queryParam, edge, score)
    }).groupBy {
      case (queryParam, edge, rank) if edge.labelWithDir.dir == GraphUtil.directions("in") =>
        (queryParam.label.srcColumn, queryParam.label.label, queryParam.label.tgtColumn, edge.tgtVertex.innerId, edge.isDegree)
      case (queryParam, edge, rank) =>
        (queryParam.label.tgtColumn, queryParam.label.label, queryParam.label.srcColumn, edge.tgtVertex.innerId, edge.isDegree)
    }

    val ret = for {
      ((tgtColumn, labelName, srcColumn, target, isDegreeEdge), edgesAndRanks) <- groupedEdgesWithRank if !isDegreeEdge
      edgesWithRanks = edgesAndRanks.groupBy(x => x._2.srcVertex).map(_._2.head)
      id <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj("name" -> tgtColumn.columnName, "id" -> id,
        SCORE_FIELD_NAME -> edgesWithRanks.map(_._3).sum,
        "label" -> labelName,
        "aggr" -> Json.obj(
          "name" -> srcColumn.columnName,
          "ids" -> edgesWithRanks.flatMap { case (queryParam, edge, rank) =>
            innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)
          },
          "edges" -> edgesWithRanks.map { case (queryParam, edge, rank) =>
            Json.obj("id" -> innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType),
              "props" -> propsToJson(edge),
              "score" -> rank
            )
          }
        )
      )
    }

    ret.toList
  }

  def sortWithFormatted(jsons: Seq[JsObject],
                        scoreField: String = "scoreSum",
                        queryRequestWithResultLs: Seq[QueryRequestWithResult],
                        decrease: Boolean = true): JsObject = {
    val ordering = if (decrease) -1 else 1
    val sortedJsons = jsons.sortBy { jsObject => (jsObject \ scoreField).as[Double] * ordering }

    if (queryRequestWithResultLs.isEmpty) Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    else Json.obj(
      "size" -> sortedJsons.size,
      "results" -> sortedJsons,
      "impressionId" -> queryRequestWithResultLs.head.queryRequest.query.impressionId()
    )
  }

  private def toHashKey(edge: Edge, queryParam: QueryParam, fields: Seq[String], delimiter: String = ","): Int = {
    val ls = for {
      field <- fields
    } yield {
      field match {
        case "from" | "_from" => edge.srcVertex.innerId
        case "to" | "_to" => edge.tgtVertex.innerId
        case "label" => edge.labelWithDir.labelId
        case "direction" => JsString(GraphUtil.fromDirection(edge.labelWithDir.dir))
        case "_timestamp" | "timestamp" => edge.ts
        case _ =>
          queryParam.label.metaPropsInvMap.get(field) match {
            case None => throw new RuntimeException(s"unknow column: $field")
            case Some(labelMeta) => edge.propsWithTs.get(labelMeta.seq) match {
              case None => labelMeta.defaultValue
              case Some(propVal) => propVal
            }
          }
      }
    }
    val ret = ls.hashCode()
    ret
  }

  def resultInnerIds(queryRequestWithResultLs: Seq[QueryRequestWithResult], isSrcVertex: Boolean = false): Seq[Int] = {
    for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      q = queryRequest.query
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
    } yield toHashKey(edge, queryRequest.queryParam, q.filterOutFields)
  }

  def summarizeWithListExcludeFormatted(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs, decrease = true)
  }

  def summarizeWithList(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs)
  }

  def summarizeWithListFormatted(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]) = {
    val jsons = groupEdgeResult(queryRequestWithResultLs, exclude)
    sortWithFormatted(jsons, queryRequestWithResultLs = queryRequestWithResultLs)
  }

  def toSimpleVertexArrJson(queryRequestWithResultLs: Seq[QueryRequestWithResult]): JsValue = {
    toSimpleVertexArrJson(queryRequestWithResultLs, Seq.empty[QueryRequestWithResult])
  }

  private def orderBy(q: Query,
                      orderByColumns: Seq[(String, Boolean)],
                      rawEdges: ListBuffer[(Map[String, JsValue], Double, (Any, Any, Any, Any))])
  : ListBuffer[(Map[String, JsValue], Double, (Any, Any, Any, Any))] = {

    if (q.withScore && orderByColumns.nonEmpty) {
      val ascendingLs = orderByColumns.map(_._2)
      rawEdges.sortBy(_._3)(new TupleMultiOrdering[Any](ascendingLs))
    } else {
      rawEdges
    }
  }

  private def getColumnValue(keyWithJs: Map[String, JsValue], score: Double, edge: Edge, column: String): Any = {
    column match {
      case "score" => score
      case "timestamp" | "_timestamp" => edge.ts
      case _ =>
        keyWithJs.get(column) match {
          case None => keyWithJs.get("props").map { js => (js \ column).as[JsValue] }.get
          case Some(x) => x
        }
    }
  }

  def toSimpleVertexArrJson(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]): JsValue = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap

    val degrees = ListBuffer[JsValue]()
    val rawEdges = ListBuffer[(Map[String, JsValue], Double, (Any, Any, Any, Any))]()

    if (queryRequestWithResultLs.isEmpty) {
      Json.obj("size" -> 0, "degrees" -> Json.arr(), "results" -> Json.arr())
    } else {
      val (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResultLs.head).get
      val query = queryRequest.query
      val queryParam = queryRequest.queryParam

      val orderByColumns = query.orderByColumns.filter { case (column, _) =>
        column match {
          case "from" | "to" | "label" | "score" | "timestamp" | "_timestamp" => true
          case _ =>
            queryParam.label.metaPropNames.contains(column)
        }
      }

      /** build result jsons */
      for {
        queryRequestWithResult <- queryRequestWithResultLs
        (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
        queryParam = queryRequest.queryParam
        edgeWithScore <- queryResult.edgeWithScoreLs
        (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
        if !excludeIds.contains(toHashKey(edge, queryRequest.queryParam, query.filterOutFields))
      } {
        // edge to json
        val (srcColumn, _) = queryParam.label.srcTgtColumn(edge.labelWithDir.dir)
        val fromOpt = innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
        if (edge.isDegree && fromOpt.isDefined) {
          degrees += Json.obj(
            "from" -> fromOpt.get,
            "label" -> queryRequest.queryParam.label.label,
            "direction" -> GraphUtil.fromDirection(edge.labelWithDir.dir),
            LabelMeta.degree.name -> innerValToJsValue(edge.propsWithTs(LabelMeta.degreeSeq).innerVal, InnerVal.LONG)
          )
        } else {
          val keyWithJs = edgeToJson(edge, score, queryRequest.query, queryRequest.queryParam)
          val orderByValues: (Any, Any, Any, Any) = orderByColumns.length match {
            case 0 =>
              (None, None, None, None)
            case 1 =>
              val it = orderByColumns.iterator
              val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              (v1, None, None, None)
            case 2 =>
              val it = orderByColumns.iterator
              val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              (v1, v2, None, None)
            case 3 =>
              val it = orderByColumns.iterator
              val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v3 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              (v1, v2, v3, None)
            case _ =>
              val it = orderByColumns.iterator
              val v1 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v2 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v3 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              val v4 = getColumnValue(keyWithJs, score, edge, it.next()._1)
              (v1, v2, v3, v4)
          }

          val currentEdge = (keyWithJs, score, orderByValues)
          rawEdges += currentEdge
        }
      }

      if (query.groupByColumns.isEmpty) {
        // ordering
        val edges = orderBy(query, orderByColumns, rawEdges).map(_._1)

        Json.obj(
          "size" -> edges.size,
          "degrees" -> degrees,
          "results" -> edges,
          "impressionId" -> query.impressionId()
        )
      } else {
        val grouped = rawEdges.groupBy { case (keyWithJs, _, _) =>
          val props = keyWithJs.get("props")

          for {
            column <- query.groupByColumns
            value <- keyWithJs.get(column) match {
              case None => props.flatMap { js => (js \ column).asOpt[JsValue] }
              case Some(x) => Some(x)
            }
          } yield column -> value
        }

        val groupedEdges = {
          for {
            (groupByKeyVals, groupedRawEdges) <- grouped
          } yield {
            val scoreSum = groupedRawEdges.map(x => x._2).sum
            // ordering
            val edges = orderBy(query, orderByColumns, groupedRawEdges).map(_._1)
            Json.obj(
              "groupBy" -> Json.toJson(groupByKeyVals.toMap),
              "scoreSum" -> scoreSum,
              "agg" -> edges
            )
          }
        }

        val groupedSortedJsons = groupedEdges.toList.sortBy { jsVal => -1 * (jsVal \ "scoreSum").as[Double] }
        Json.obj(
          "size" -> groupedEdges.size,
          "degrees" -> degrees,
          "results" -> groupedSortedJsons,
          "impressionId" -> query.impressionId()
        )
      }
    }
  }

  def verticesToJson(vertices: Iterable[Vertex]) = {
    Json.toJson(vertices.flatMap { v => vertexToJson(v) })
  }

  def propsToJson(edge: Edge, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    for {
      (seq, innerValWithTs) <- edge.propsWithTs if LabelMeta.isValidSeq(seq)
      labelMeta <- queryParam.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(innerValWithTs.innerVal, labelMeta.dataType)
    } yield labelMeta.name -> jsValue
  }

  private def edgeParent(parentEdges: Seq[EdgeWithScore], q: Query, queryParam: QueryParam): JsValue = {
    if (parentEdges.isEmpty) {
      JsNull
    } else {
      val parents = for {
        parent <- parentEdges
        (parentEdge, parentScore) = EdgeWithScore.unapply(parent).get
        parentQueryParam = QueryParam(parentEdge.labelWithDir)
        parents = edgeParent(parentEdge.parentEdges, q, parentQueryParam) if parents != JsNull
      } yield {
        val originalEdge = parentEdge.originalEdgeOpt.getOrElse(parentEdge)
        val edgeJson = edgeToJsonInner(originalEdge, parentScore, q, parentQueryParam) + ("parents" -> parents)
        Json.toJson(edgeJson)
      }

      Json.toJson(parents)
    }
  }

  /** TODO */
  def edgeToJsonInner(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    val (srcColumn, tgtColumn) = queryParam.label.srcTgtColumn(edge.labelWithDir.dir)

    val kvMapOpt = for {
      from <- innerValToJsValue(edge.srcVertex.id.innerId, srcColumn.columnType)
      to <- innerValToJsValue(edge.tgtVertex.id.innerId, tgtColumn.columnType)
    } yield {
      val targetColumns = if (q.selectColumnsSet.isEmpty) reservedColumns else (reservedColumns & q.selectColumnsSet) + "props"

      val _propsMap = queryParam.label.metaPropsDefaultMapInner ++ propsToJson(edge, q, queryParam)
      val propsMap = if (q.selectColumnsSet.nonEmpty) _propsMap.filterKeys(q.selectColumnsSet) else _propsMap

      val kvMap = targetColumns.foldLeft(Map.empty[String, JsValue]) { (map, column) =>
        val jsValue = column match {
          case "cacheRemain" => JsNumber(queryParam.cacheTTLInMillis - (System.currentTimeMillis() - queryParam.timestamp))
          case "from" => from
          case "to" => to
          case "label" => JsString(queryParam.label.label)
          case "direction" => JsString(GraphUtil.fromDirection(edge.labelWithDir.dir))
          case "_timestamp" | "timestamp" => JsNumber(edge.ts)
          case "score" => JsNumber(score)
          case "props" if propsMap.nonEmpty => Json.toJson(propsMap)
          case _ => JsNull
        }

        if (jsValue == JsNull) map else map + (column -> jsValue)
      }
      kvMap
    }

    kvMapOpt.getOrElse(Map.empty)
  }

  def edgeToJson(edge: Edge, score: Double, q: Query, queryParam: QueryParam): Map[String, JsValue] = {
    val kvs = edgeToJsonInner(edge, score, q, queryParam)
    if (kvs.nonEmpty && q.returnTree) kvs + ("parents" -> Json.toJson(edgeParent(edge.parentEdges, q, queryParam)))
    else kvs
  }

  def vertexToJson(vertex: Vertex): Option[JsObject] = {
    val serviceColumn = ServiceColumn.findById(vertex.id.colId)

    for {
      id <- innerValToJsValue(vertex.innerId, serviceColumn.columnType)
    } yield {
      Json.obj("serviceName" -> serviceColumn.service.serviceName,
        "columnName" -> serviceColumn.columnName,
        "id" -> id, "props" -> propsToJson(vertex),
        "timestamp" -> vertex.ts,
        //        "belongsTo" -> vertex.belongLabelIds)
        "belongsTo" -> vertex.belongLabelIds.flatMap(Label.findByIdOpt(_).map(_.label)))
    }
  }

  private def keysToName(seqsToNames: Map[Int, String], props: Map[Int, InnerValLike]) = {
    for {
      (seq, value) <- props
      name <- seqsToNames.get(seq)
    } yield (name, value)
  }

  private def propsToJson(vertex: Vertex) = {
    val serviceColumn = vertex.serviceColumn
    val props = for {
      (propKey, innerVal) <- vertex.props
      columnMeta <- ColumnMeta.findByIdAndSeq(serviceColumn.id.get, propKey.toByte, useCache = true)
      jsValue <- innerValToJsValue(innerVal, columnMeta.dataType)
    } yield {
      (columnMeta.name -> jsValue)
    }
    props.toMap
  }

  @deprecated(message = "deprecated", since = "0.2")
  def propsToJson(edge: Edge) = {
    for {
      (seq, v) <- edge.propsWithTs if LabelMeta.isValidSeq(seq)
      metaProp <- edge.label.metaPropsMap.get(seq)
      jsValue <- innerValToJsValue(v.innerVal, metaProp.dataType)
    } yield {
      (metaProp.name, jsValue)
    }
  }

  @deprecated(message = "deprecated", since = "0.2")
  def summarizeWithListExclude(queryRequestWithResultLs: Seq[QueryRequestWithResult], exclude: Seq[QueryRequestWithResult]): JsObject = {
    val excludeIds = resultInnerIds(exclude).map(innerId => innerId -> true).toMap


    val groupedEdgesWithRank = (for {
      queryRequestWithResult <- queryRequestWithResultLs
      (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
      edgeWithScore <- queryResult.edgeWithScoreLs
      (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
      if !excludeIds.contains(toHashKey(edge, queryRequest.queryParam, queryRequest.query.filterOutFields))
    } yield {
      (edge, score)
    }).groupBy { case (edge, score) =>
      (edge.label.tgtColumn, edge.label.srcColumn, edge.tgtVertex.innerId)
    }

    val jsons = for {
      ((tgtColumn, srcColumn, target), edgesAndRanks) <- groupedEdgesWithRank
      (edges, ranks) = edgesAndRanks.groupBy(x => x._1.srcVertex).map(_._2.head).unzip
      tgtId <- innerValToJsValue(target, tgtColumn.columnType)
    } yield {
      Json.obj(tgtColumn.columnName -> tgtId,
        s"${srcColumn.columnName}s" ->
          edges.flatMap(edge => innerValToJsValue(edge.srcVertex.innerId, srcColumn.columnType)), "scoreSum" -> ranks.sum)
    }
    val sortedJsons = jsons.toList.sortBy { jsObj => (jsObj \ "scoreSum").as[Double] }.reverse
    if (queryRequestWithResultLs.isEmpty) {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons)
    } else {
      Json.obj("size" -> sortedJsons.size, "results" -> sortedJsons,
        "impressionId" -> queryRequestWithResultLs.head.queryRequest.query.impressionId())
    }

  }
}

//trait Service extends RequestParser {
////  implicit val system: ActorSystem
////
////  implicit def executor: ExecutionContextExecutor
////
////  implicit val materializer: Materializer
//
//  def config: Config
//
//  val numOfThread = Runtime.getRuntime.availableProcessors()
//  val threadPool = Executors.newFixedThreadPool(numOfThread)
//  val ec = ExecutionContext.fromExecutor(threadPool)
//
//  lazy val s2: Graph = new Graph(config)(ec)
//
//  val logger: LoggingAdapter
//
//  val routes = {
//    logRequestResult("akka-http") {
//      pathPrefix("ping") {
//        pathEnd {
//          get {
//            println("pong")
//            complete("pong")
//          }
//        }
//      } ~
//        pathPrefix("graphs") {
//          pathPrefix("getEdges") {
//            pathEnd {
//              post {
//                entity(as[String]) { payload =>
//                  val bodyAsJson = Json.parse(payload)
//                  val query = toQuery(bodyAsJson)
//                  val fetch = s2.getEdges(query)
//
//                  complete {
//                    fetch.map { queryRequestWithResutLs =>
//                      val jsValue = PostProcess.toSimpleVertexArrJson(queryRequestWithResutLs, Nil)
//                      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), jsValue.toString))
//                    }
//                  }
//                }
//              }
//            }
//          }
//        }
//    }
//  }
//}

object S2Rest extends App with RequestParser {
  //  override implicit val system = ActorSystem()
  //  override implicit val executor = system.dispatcher
  //  override implicit val materializer = ActorMaterializer()

  //  override val config = ConfigFactory.load()
  //  override val logger = Logging(system, getClass)

  val config = ConfigFactory.load()
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  val s2: Graph = new Graph(config)(ec)

  ServiceColumn.findAll()
  Label.findAll()
  LabelMeta.findAll()
  LabelIndex.findAll()
  ColumnMeta.findAll()

  val interface = try config.getString("http.interface") catch {
    case _: Throwable => "0.0.0.0"
  }
  val port = try config.getString("http.port") catch {
    case _: Throwable => "9000"
  }

  //  Http().bindAndHandle(routes, interface, port.toInt)
  import com.twitter.finagle._
  import com.twitter.util._

  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] = {
      val promise = new com.twitter.util.Promise[http.Response]
      val payload = req.contentString
      val bodyAsJson = Json.parse(payload)
      val query = toQuery(bodyAsJson)
      val fetch = s2.getEdges(query)

      fetch.onComplete {
        case Success(queryRequestWithResutLs) =>
          val jsValue = PostProcess.toSimpleVertexArrJson(queryRequestWithResutLs, Nil)
          val httpRes = {
            val response = com.twitter.finagle.http.Response(
              com.twitter.finagle.http.Version.Http11,
              com.twitter.finagle.http.Status.Ok)
            response.setContentTypeJson()
            response.setContentString(jsValue.toString)
            response
          }
          promise.become(Future.value(httpRes))
      }

      promise

      //    val bodyAsJson = Json.parse(payload)
      //    val query = toQuery(bodyAsJson)
      //    val fetch = s2.getEdges(query)
      //
      //    complete {
      //      fetch.map { queryRequestWithResutLs =>
      //        val jsValue = PostProcess.toSimpleVertexArrJson(queryRequestWithResutLs, Nil)
      //        HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), jsValue.toString))
      //      }
      //    }

      //      Future.value {
      //        println("ok")
      //        http.Response(req.version, http.Status.Ok)
      //      }
    }
  }

  val server = com.twitter.finagle.Http.serve(s":$port", service)
  Await.ready(server)
}
