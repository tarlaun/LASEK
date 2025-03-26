# Development

This page describes how to run a development version of Beast. That is, a version other than the released ones.
This is useful if you want to make a modification inside Beast or add a new features.
If your goal is to use existing Beast functions in your own program, it is recommended to create your own
project as explained [here](dev-setup.md).
The process is to download the code, compile it, install it in the local Maven repository, and create a binary package.
The details are below.

## Prerequisites
1. JDK
2. Maven
3. Git

## Download the code
Grab a copy of the code from git. For example:
```shell
git clone https://bitbucket.org/bdlabucr/beast.git
```

## Compile and install
For full compilation and installation, run the following line:
```shell
mvn clean install -DskipTests
```
This will compile the code, install it to the local Maven repository, and produce a binary package under the `target/` directory.

## Use in development
If you are writing a program that depends on Beast and would like to use a development version,
you need to modify the `pom.xml` file of your project to be the same version that you compiled.
Typically, this will be a version that ends in `-SNAPSHOT` since it is not released.

## Use in command line
To use the command line interface of the development version, run the `beast` command under `target/beast-{version}/bin`
where `{version}` is the version of your development version. You can either copy that directory somewhere else to have
a stable version, or run it directly from the `target/` directory. 