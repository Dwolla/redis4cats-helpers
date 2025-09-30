ThisBuild / tlBaseVersion := "0.1"

ThisBuild / organization := "com.dwolla"
ThisBuild / organizationName := "Dwolla"
ThisBuild / startYear := Some(2025)
ThisBuild / licenses := Seq(License.MIT)
ThisBuild / developers := List(
  tlGitHubDev("bpholt", "Brian Holt")
)

val Scala3 = "3.3.6"
ThisBuild / crossScalaVersions := Seq(Scala3, "2.13.16", "2.12.20")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head // the default Scala
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"))
ThisBuild / githubWorkflowScalaVersions := Seq("3", "2.13", "2.12")
ThisBuild / tlJdkRelease := Some(8)
ThisBuild / tlCiReleaseBranches := Seq("main")
ThisBuild / mergifyStewardConfig ~= { _.map {
  _.withAuthor("dwolla-oss-scala-steward[bot]")
    .withMergeMinors(true)
}}
ThisBuild / resolvers += Resolver.sonatypeCentralSnapshots

lazy val root = tlCrossRootProject.aggregate(
  `redis4cats-transactional-updates`,
  `redis4cats-localcache`
)

lazy val `redis4cats-transactional-updates` = project.in(file("core"))
  .settings(
    name := "redis4cats-transactional-updates",
    description := "Uses redis4cats to interact with the Redis transactions API to update or delete existing keys if and only if their value is the same as some expected value.",
    libraryDependencies ++= {
      Seq(
        "dev.profunktor" %% "redis4cats-effects" % "2.0.1",
        "org.tpolecat" %% "natchez-core" % "0.3.8",
        "io.circe" %% "circe-core" % "0.14.15",
        "io.circe" %% "circe-literal" % "0.14.15",
        "org.typelevel" %% "cats-tagless-core" % "0.16.3",
        "org.typelevel" %% "scalac-compat-annotation" % "0.1.4",
        "com.dwolla" %%% "scala2-notgiven-compat" % "0.1-90f5de2-SNAPSHOT",
      ) ++ Seq(
        "org.typelevel" %% "cats-tagless-macros" % "0.16.3",
      ).filter(_ => scalaVersion.value.startsWith("2"))
    },
    scalacOptions ++= Seq(
      "-source:future",
    ).filter(_ => scalaVersion.value.startsWith("3")),
  )

lazy val `redis4cats-localcache` = project
  .in(file("localcache"))
  .settings(
    description := "Provides a fake in-memory implementation of Redis semantics for Get/Set/Delete/UpdateExisting/DeleteExisting using Cats Effect.",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "keypool" % "0.4.10",
      "com.github.cb372" %% "cats-retry" % "3.1.3",
    ),
    scalacOptions ++= Seq(
      "-source:future",
    ).filter(_ => scalaVersion.value.startsWith("3")),
  )
  .dependsOn(`redis4cats-transactional-updates`)
