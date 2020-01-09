val commonSettings = Seq(
  organization := "se.kth.benchmarks",
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.12.10",
  scalacOptions ++= Seq("-deprecation", "-feature"),
  resolvers += Resolver.mavenLocal
);

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "Benchmark Suite Visualisation"
  )
  .aggregate(plotting, generator);

lazy val plotting = (project in file("plotting"))
  .enablePlugins(ScalaJSPlugin, ScalaJSWeb)
  .settings(
    commonSettings,
    name := "Benchmark Suite Plotting",
    libraryDependencies ++= Seq(
      "com.outr" %%% "scribe" % "2.7.3",
      "com.lihaoyi" %%% "scalatags" % "0.8.2",
      "org.scala-js" %%% "scalajs-dom" % "0.9.7",
      "com.github.karasiq" %%% "scalajs-highcharts" % "1.2.1",
      "org.scalatest" %%% "scalatest" % "3.1.0" % "test"
    )
    //mainClass in Compile := Some("se.kth.benchmarks.visualisation.plotting.Plotting"),
    //scalaJSUseMainModuleInitializer := true
  );

lazy val generator = (project in file("generator"))
  .enablePlugins(SbtWeb)
  .settings(
    commonSettings,
    name := "Benchmark Suite Visualisation Generator",
    libraryDependencies ++= Seq(
      "org.rogach" %% "scallop" % "3.3.2",
      "com.lihaoyi" %% "scalatags" % "0.8.2",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.+",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.github.tototoshi" %% "scala-csv" % "1.3.6",
      "se.kth.benchmarks" %% "benchmark-suite-runner" % "0.3.0-SNAPSHOT",
      "org.scalatest" %% "scalatest" % "3.1.0" % "test"
    ),
    scalaJSProjects := Seq(plotting),
    pipelineStages in Assets := Seq(scalaJSPipeline),
    WebKeys.packagePrefix in Assets := "public/",
    managedClasspath in Runtime += (packageBin in Assets).value,
    fork := true,
    sourceGenerators in Compile += Def.task {
      convertScripts(baseDirectory.value, (sourceManaged in Compile).value / "scripts")
    }.taskValue
  );

// TODO make this incremental
def convertScripts(root: File, base: File): Seq[File] = {
  val target = base /  "package.scala";
  println(s"Base: ${root}");
  val src = IO.readLines(root / ".." / ".." / "benchmarks.sc").toArray;
  var lines = scala.collection.mutable.ArrayBuffer(src:_*);
  lines = lines.dropWhile(l => !l.contains("val implementations"));
  lines = lines.takeWhile(l => !l.contains("implicit class"));
  lines = lines.filterNot(l => 
    l.contains("local") 
    || l.contains("remote") 
    || l.contains("client") 
    || l.contains("mustCopy"))
  lines = lines.map(l => l.replaceAll("BenchmarkImpl", "BenchmarkInfo"))
  lines.prepend("case class BenchmarkInfo(symbol: String, label: String)");
  lines.prepend("package object scripts {");
  lines.prepend("package se.kth.benchmarks");
  lines.append("}");
  
  // IO.write(target, """
  //   package se.kth.benchmarks

  //   package object scripts {
  //    val implementations: Map[String, String] = Map("test" -> "value");
  //   }
  //   """)
  IO.write(target, lines.mkString("\n"));
  Seq(target)
}