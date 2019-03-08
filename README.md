# ScalableClustering

Steps to create sbt project using command line:

1:Install sbt

2.Create new folder with some name
mkdir newProject
cd newProject

3.Create build file:
vi build.sbt

include the scala version and dependencies in  build.sbt file:
eg:
scalaVersion := "2.11.11"
name := "scalab-clus"
//organization := ""
version := "1.0"
dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"


4.create two folders
mkdir src/main/scala
mkdir src/test/scala
