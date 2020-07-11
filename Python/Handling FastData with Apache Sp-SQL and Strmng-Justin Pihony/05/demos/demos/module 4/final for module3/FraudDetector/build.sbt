name := "Fraud Detector"

version := "0.1"

scalaVersion := "2.11.8"

sparkVersion := "2.1.0"

sparkComponents += "sql"

spDependencies += "datastax/spark-cassandra-connector:2.0.1-s_2.11"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)	