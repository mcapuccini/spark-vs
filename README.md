# Spark SBVS #

Spark-VS is a library for setting up massively parallel Structure-Based Virtual Screening (SBVS) pipelines in Spark.

## Getting started ##

The following procedure was tested on Ubuntu 14.04.2 LTS. We assume you downloaded OpenEye-Java-2015.Feb.3-Linux-x64 and own its license.

### Install OpenEye in yur local maven repository ###

```
cd OpenEye-Java-2015.Feb.3-Linux-x64/lib/
mvn install:install-file -DgroupId=OpenEye -DartifactId=java_api -Dversion=2015Feb3 -Dpackaging=jar -Dfile=oejava-2015.Feb.3-Linux-x64.jar
mvn install:install-file -DgroupId=OpenEye -DartifactId=java_utils -Dversion=2015Feb3 -Dpackaging=jar -Dfile=openeye-javautils.jar
```

### Install SDF and SMILES parsers for Spark ###
```
git clone git@bitbucket.org:mcapuccini/parsers.git
cd parsers/parsers/
mvn install -DskipTests
```

### Install Spark-VS in your local maven repository ###
```
git clone git@bitbucket.org:olas/spark-vs.git
cd spark-vs/vs
mvn install -DskipTests
```

### Import vs.examples project in Scala IDE ###

1. File > Import > General > Existing project into workspace
2. Select vs.example as root directory
3. Click finish
4. Wait for the workspace to build (this can take a while)
  If the IDE asks to include the scala library or compiler in the workspace click No

If you have scala version problems follow this procedure:

1. Right click on the project foldel in the Package Explorer > Properties > Scala Compiler 
2. Select fixed scala installation 2.10.X
3. Click apply and let the IDE clean the project


Now you can get familiar with Spark-VS giving a look to the examples, and running them in Scala IDE. 
In the data directory you can find an exaple SDF and SMILES files, as well as a receptor file. 
Remember that in order to run examples you need to specify arguments and OE_LICENSE environment variable through Run Configurations. 
Later on you may want to create your own project to write specific pipelines for your use case.