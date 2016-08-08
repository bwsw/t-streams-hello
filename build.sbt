name 		:= "t-streams-hello"
version 	:= "1.0"
scalaVersion 	:= "2.11.8"

resolvers +=
"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies = Seq("com.bwsw" % "t-streams_2.11" % "1.0-SNAPSHOT")

assemblyJarName in assembly := "ts-hello" + version + ".jar"
