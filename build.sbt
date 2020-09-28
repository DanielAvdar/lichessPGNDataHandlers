name := "PGNDATAS"


version := "0.1"

scalaVersion := "2.12.4"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Spark Packages Repo" at "https://mvnrepository.com/artifact/org.apache.spark/spark-streaming"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"




libraryDependencies += "org.neo4j" % "neo4j-common" % "3.5.22" % Test

libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.7.5"

libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.4.5-M2"
