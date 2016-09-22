val prodVersion = "1.0"
name 		:= "t-streams-hello"
version 	:= prodVersion
scalaVersion 	:= "2.11.8"

resolvers += "twitter resolver" at "http://maven.twttr.com"
resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq("com.bwsw" % "t-streams_2.11" % "1.0.3.1")
assemblyJarName in assembly := "ts-hello" + prodVersion + ".jar"
