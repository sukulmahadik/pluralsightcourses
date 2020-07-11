name := "KafkaTransactionLogProducer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.2.0"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)	