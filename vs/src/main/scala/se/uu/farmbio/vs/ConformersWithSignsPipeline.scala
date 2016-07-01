package se.uu.farmbio.vs

import org.apache.commons.lang.NotImplementedException
import org.apache.spark.rdd.RDD

trait ConformersWithSignsTransforms {
  def dockWithML(path: String)
}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {
  def dockWithML(path: String) = {
    throw new NotImplementedException("Please don't call me again. I am not ready yet")
  }
}