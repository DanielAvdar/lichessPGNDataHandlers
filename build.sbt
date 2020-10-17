
name := "PGNDATAS"


version := "0.1"

scalaVersion := "2.12.4"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
//resolvers += "Spark Packages Repo" at "https://mvnrepository.com/artifact/org.apache.spark/spark-streaming"
//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
//libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "4.0.0"


//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"% "provided"
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7"% "provided"
//
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"% "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"% "provided"
//
//libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.17.3"% "provided"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"

//libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.17.3"



//libraryDependencies += "org.neo4j" % "neo4j-common" % "3.5.22" % Test

//libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.7.5"

//libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.4.5-M2"
//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
//libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "4.0.0"