package se.uu.farmbio.vs

import java.io.PrintWriter
import java.nio.file.Paths

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD

trait ConformersWithSignsTransforms {
 
}

object ConformerWithSigns {

private[vs] class ConformersWithSigns(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  

}

}