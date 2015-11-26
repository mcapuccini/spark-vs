package se.uu.farmbio.vs

import java.io.InputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import uu.farmbio.sg.SGUtils



trait ConformerTransforms {

  def dock(receptor: InputStream, method: Int, resolution: Int): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms
  

}

class ConformerPipeline[vs](override val rdd: RDD[String])
  extends SBVSPipeline(rdd) with ConformerTransforms {
    
  override def dock(receptor: InputStream, method: Int, resolution: Int) = {
      val receptorBytes = IOUtils.toByteArray(receptor)
      val bcastReceptor = sc.broadcast(receptorBytes)
      val res = rdd.flatMap(OEChemLambdas.oeDocking(bcastReceptor, method, resolution, oeErrorLevel))
      new PosePipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

  
}