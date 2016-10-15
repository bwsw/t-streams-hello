val prodVersion = "1.1"
name 		:= "t-streams-hello"
version 	:= prodVersion
scalaVersion 	:= "2.11.8"

resolvers += Resolver.sonatypeRepo("snapshots")


libraryDependencies ++= Seq("com.bwsw" % "t-streams_2.11" % "1.1.0-SNAPSHOT")
assemblyJarName in assembly := "ts-hello" + prodVersion + ".jar"
