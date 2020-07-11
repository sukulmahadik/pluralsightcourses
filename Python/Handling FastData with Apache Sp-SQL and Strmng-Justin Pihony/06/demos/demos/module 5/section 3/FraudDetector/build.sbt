name := "Fraud Detector"

version := "0.1"

scalaVersion := "2.11.8"

sparkVersion := "2.1.0"

sparkComponents ++= Seq("sql", "streaming")

spDependencies += "datastax/spark-cassandra-connector:2.0.1-s_2.11"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)	

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}