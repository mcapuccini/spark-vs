<p align="center"><img src="logo.png" width="500px"/></p>

**Spark-VS** is a Spark-based library for setting up massively parallel Structure-Based Virtual Screening (SBVS) pipelines in Spark.

## Getting started 
First, you need to setup a Spark project with maven, this tutorial is a good starting point: www.youtube.com/watch?v=aB4-RD_MMf0

Then, add the following entries into your pom.xml file: 

	<repositories>
		...
		<repository>
			<id>pele.farmbio.uu.se</id>
			<url>http://pele.farmbio.uu.se/artifactory/libs-snapshot</url>
		</repository>
		...
	</repositories>

	<dependencies>
	...
		<groupId>se.uu.farmbio</groupId>
			<artifactId>vs</artifactId>
			<version>0.0.1-SNAPSHOT</version>
		</dependency>
	...
	</dependencies>
	
Finally, since OpenEye libraries are used under the hood, you need to own and a OpenEye license in order to run this. Therefore, you need to set a OE_LICENSE environment variable that points to the license, in your system to run the code in this repository.	

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
