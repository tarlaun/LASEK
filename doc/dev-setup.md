# Setup the development environment for Beast

This page describes how to use Beast in your development environment.
This can help you, for example, create new input/output formats, new partitioners, or just use the features
of Beast in your own Spark program.

## Prerequisites

In order to use Beast, you need the following prerequisites installed on your machine.

* Java Development Kit (JDK). [Oracle JDK](https://www.oracle.com/technetwork/java/javase/downloads/index.html) 1.8 or later is recommended.
* [Apache Maven](https://maven.apache.org/) or [SBT](https://www.scala-sbt.org).

## Create a new project for Beast

The easiest way to start developing for Beast is to create a new project with following Maven command.
```shell
mvn archetype:generate -B -DgroupId=com.example.beastExample -DartifactId=beast-project \
    -DarchetypeGroupId=edu.ucr.cs.bdlab -DarchetypeArtifactId=distribution -DarchetypeVersion=0.10.1-RC2
```

The created project will have a template code for both Java and Scala where you can start developing.

## Configure an existing project

If you have an existing Maven-based project, then you can integrate it with Beast by
adding the following dependency to your `pom.xml` file.
```xml
<!-- https://mvnrepository.com/artifact/edu.ucr.cs.bdlab/beast -->
<dependency>
  <groupId>edu.ucr.cs.bdlab</groupId>
  <artifactId>beast-spark</artifactId>
  <version>0.10.1-RC2</version>
</dependency>
```

## Run your code in the IDE
While developing, it is recommended to run your code in the IDE. If you wrote your own main class, you can directly
run it and the IDE will integrate all Beast libraries correctly.
Alternatively, you can run the existing Main class that ships with Beast which gives you access to the prepacked
functions, e.g., index and plot. Just create a new run configuration and set the main class to
`edu.ucr.cs.bdlab.beast.operations.Main`.

## Run from command line

When your code is ready, you can run it from the command-line on a configured Spark cluster.
First, you can package your code into a JAR file by running the command `mvn package`
This will generate a new JAR under `target/` directory.

After that, you can use the `beast` command to run this JAR file. If the JAR file is configured with a main class,
you can run it with the following command.
```shell
beast target/my-app.jar
```

If you want to use existing Beast commands but still integrate your JAR file, you can use the following line
which, as an example, runs the index command.
```shell
beast --jars target/my-app.jar index ...
```

Finally, if your JAR file is not configured with a main class but you want to run a specific class, use the following
command:
```shell
beast --class <class name> target/my-app.jar
```
