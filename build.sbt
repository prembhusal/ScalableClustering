scalaVersion := "2.11.11"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings" Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.12.4"

// It's possible to define many kinds of settings, such as:

name := "scalab-clus"
//organization := ""
version := "1.0"


dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
