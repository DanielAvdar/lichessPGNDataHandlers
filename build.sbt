
name := "PGNDATAS"


version := "0.1"

scalaVersion := "2.12.4"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"


//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" %  Provided
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7"%  Provided
//
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"%  Provided
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"%  Provided

