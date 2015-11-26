package se.uu.farmbio.vs

import java.io.InputStream

import org.apache.commons.lang.NotImplementedException
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import uu.farmbio.sg.SGUtils


trait ConformerTransforms {

  def dock(receptor: InputStream, method: Int, resolution: Int): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms
  def generateSignatures() : ICPMLTransforms

}

class ConformerPipeline[vs](override val rdd: RDD[String])
  extends SBVSPipeline(rdd) with ConformerTransforms {
    
  override def dock(receptor: InputStream, method: Int, resolution: Int) = {
//    val receptorBytes = IOUtils.toByteArray(receptor)
//    val bcastReceptor = sc.broadcast(receptorBytes)
//    val res = rdd.flatMap(OEChemLambdas.oeDocking(bcastReceptor, method, resolution, oeErrorLevel))
//    new PosePipeline(res)
      throw new NotImplementedException("Needs to be reimplemented due to memory issue")
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

  def generateSignatures = {
    val molsCount = rdd.count()
    val molsWithIndex = rdd.zipWithIndex()
    val molsAfterSG = molsWithIndex.flatMap{case(mol, index) => Sdf2LibSVM.sdf2signatures(mol,index + 1,molsCount)} //Compute signatures
    .cache
    val (result, sig2ID_universe) = SGUtils.sig2ID_carryData(molsAfterSG)
    val resultAsLP : RDD[(Long, LabeledPoint)] = SGUtils.sig2LP_carryData(result, sc);

    new ICPMLPipeline(resultAsLP)
 }
}