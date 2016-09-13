val prodVersion = "1.0"
name 		:= "t-streams-hello"
version 	:= prodVersion
scalaVersion 	:= "2.11.8"
resolvers +=
"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "twitter resolver" at "http://maven.twttr.com"


libraryDependencies ++= Seq("com.bwsw" % "t-streams_2.11" % "1.0-SNAPSHOT")
assemblyJarName in assembly := "ts-hello" + prodVersion + ".jar"
