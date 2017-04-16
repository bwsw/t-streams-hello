val prodVersion = "1.1"
name 		:= "t-streams-hello"
version 	:= prodVersion
scalaVersion 	:= "2.12.1"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq("com.bwsw" % "t-streams_2.12" % "2.1.5-SNAPSHOT")
assemblyJarName in assembly := "ts-hello" + prodVersion + ".jar"
