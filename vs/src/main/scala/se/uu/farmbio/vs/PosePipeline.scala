package se.uu.farmbio.vs

import scala.io.Source
import org.apache.spark.rdd.RDD
import openeye.oedocking.OEDockMethod
import org.apache.log4j.Logger

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with PoseTransforms

}

private[vs] object PosePipelineLogger {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

private[vs] class PosePipeline(override val rdd: RDD[String], val scoreMethod: Int) extends SBVSPipeline(rdd)
    with PoseTransforms {

  val methodBroadcast = rdd.sparkContext.broadcast(scoreMethod)

  private def parseId = (pose: String) => {
    Source.fromString(pose).getLines.next
  }

  private def parseScore(method: Int) = (pose: String) => {
    var result: Double = Double.MinValue
    //Sometimes OEChem produce molecules with empty score or malformed molecules
    //We use try catch block for those exceptions
    try {
      val methodString: String = method match {
        case OEDockMethod.Chemgauss4 => "Chemgauss4"
        case OEDockMethod.Chemgauss3 => "Chemgauss3"
        case OEDockMethod.Shapegauss => "Shapegauss"
        case OEDockMethod.Chemscore  => "Chemscore"
        case OEDockMethod.Hybrid     => "Hybrid"
        case OEDockMethod.Hybrid1    => "Hybrid1"
        case OEDockMethod.Hybrid2    => "Hybrid2"
        case OEDockMethod.PLP        => "PLP"
      }
      var res: String = null
      val it = SBVSPipeline.CDKInit(pose)
      while (it.hasNext()) {
        val mol = it.next
        res = mol.getProperty(methodString)
      }
      result = res.toDouble
    } catch {

      case nfe: NumberFormatException => PosePipelineLogger.log
        .warn("EmptyScore : Setting the score to Double.MinValue." +
          "It was not possible to parse the score of the following molecule due to \n" +
          nfe.getStackTraceString + "\nPose:\n" + pose)
      case excep: Exception => PosePipelineLogger.log
      .warn("Malformed mol: Setting the score to Double.MinValue." +
          "It was not possible to parse the score of the following molecule due to \n" +
          excep.getStackTraceString + "\nPose:\n" + pose)
    }
    result
  }

  private def collapsePoses(bestN: Int, parseScore: String => Double) = (record: (String, Iterable[String])) => {
    record._2.toList.sortBy(parseScore).reverse.take(bestN)
  }

  override def sortByScore = {
    val res = rdd.sortBy(parseScore(methodBroadcast.value), false)
    new PosePipeline(res, scoreMethod)
  }

  override def collapse(bestN: Int) = {
    val res = rdd.groupBy(parseId)
      .flatMap(collapsePoses(bestN, parseScore(methodBroadcast.value)))
    new PosePipeline(res, scoreMethod)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new PosePipeline(res, scoreMethod)
  }

}