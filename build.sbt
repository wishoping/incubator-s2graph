import spray.revolver.RevolverPlugin._

name := "s2graph"

lazy val commonSettings = Seq(
  organization := "com.kakao.s2graph",
  scalaVersion := "2.11.7",
  version := "0.12.1-SNAPSHOT",
  scalacOptions := Seq("-language:postfixOps", "-unchecked", "-deprecation", "-feature", "-Xlint"),
  javaOptions ++= collection.JavaConversions.propertiesAsScalaMap(System.getProperties).map{ case (key, value) => "-D" + key + "=" + value }.toSeq,
  testOptions in Test += Tests.Argument("-oDF"),
  parallelExecution in Test := false,
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )
)

lazy val root = project.in(file("."))
  .dependsOn(s2core)
  .settings(commonSettings: _*)

lazy val s2core = project.settings(commonSettings: _*)

libraryDependencies ++= Seq(
  "xalan" % "serializer" % "2.7.2", // Download in Intelli J(Download Source/Document)
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3",
  "org.json4s" %% "json4s-native" % "3.2.11" % Test
)

libraryDependencies ++= {
  val akkaV       = "2.3.12"
  val akkaStreamV = "1.0"
  val scalaTestV  = "2.2.5"

  Seq(
    "com.typesafe.akka" %% "akka-actor"                           % akkaV,
    "com.typesafe.akka" %% "akka-stream-experimental"             % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental"          % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-experimental"               % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental"    % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-testkit-experimental"       % akkaStreamV,
    "org.scalatest"     %% "scalatest"                            % scalaTestV % "test"
  )
}

Revolver.settings
