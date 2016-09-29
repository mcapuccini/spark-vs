package se.uu.farmbio.vs

import scala.io.Source

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import openeye.oedocking.OEDockMethod

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with PoseTransforms
  def getTopPoses(topN: Int): Array[String]

}

private[vs] object PosePipeline extends Logging {

  private def parseId(pose: String) = {
    Source.fromString(pose).getLines.next
  }

  private[vs] def parseIdAndScore(method: Int)(pose: String) = {
    var score: Double = Double.MinValue
    val id: String = parseId(pose)
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
      if (it.hasNext()) {
        val mol = it.next
        res = mol.getProperty(methodString)

      }
      score = res.toDouble
    } catch {

      case exec: Exception => logWarning("Setting the score to Double.MinValue." +
        "It was not possible to parse the score of the following molecule due to \n" + exec +
        "\n" + exec.getStackTraceString + "\nPose:\n" + pose)

    }
    (id, score)

  }

  private def parseScore(method: Int)(pose: String) = {
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
      if (it.hasNext()) {
        val mol = it.next
        res = mol.getProperty(methodString)
      }
      result = res.toDouble
    } catch {

      case exec: Exception => logWarning("Setting the score to Double.MinValue." +
        "It was not possible to parse the score of the following molecule due to \n" + exec +
        "\n" + exec.getStackTraceString + "\nPose:\n" + pose)

    }
    result
  }
  @deprecated("parent Method collapse deprecated", "Sep 29, 2016")
  private def collapsePoses(bestN: Int, parseScore: String => Double) = (record: (String, Iterable[String])) => {
    record._2.toList.sortBy(parseScore).reverse.take(bestN)
  }

}

private[vs] class PosePipeline(override val rdd: RDD[String], val scoreMethod: Int) extends SBVSPipeline(rdd)
    with PoseTransforms {

  //Need a local copy due to serialization error 
  // http://spark-summit.org/wp-content/uploads/2013/10/McDonough-spark-tutorial_spark-summit-2013.pptx
  val methodBroadcast = rdd.sparkContext.broadcast(scoreMethod)

  override def getTopPoses(topN: Int) = {
    val methodBroadcastLocal = methodBroadcast
    val method = methodBroadcastLocal.value
    //Parsing id and Score in parallel and collecting data to driver
    val idAndScore = rdd.map {
      case (mol) => PosePipeline.parseIdAndScore(method)(mol)
    }.collect()

    //Finding Distinct top id and score in serial at driver
    val topMols =
      idAndScore.foldLeft(Map[String, Double]() withDefaultValue Double.MinValue) {
        case (m, (id, score)) => m updated (id, score max m(id))
      }
      .toSeq
      .sortBy { case (id, score) => -score }
      .take(topN).toArray

    //Broadcasting the top id and score and search main rdd
    //for top molecules in parallel  
    val topMolsBroadcast = rdd.sparkContext.broadcast(topMols)
    val topPoses = rdd.filter { mol =>
      val idAndScore = PosePipeline.parseIdAndScore(method)(mol)
      topMolsBroadcast.value
        .map(topHit => topHit == idAndScore)
        .reduce(_ || _)
    }
    //return statement  
    topPoses.collect
      .sortBy {
        mol => -PosePipeline.parseScore(methodBroadcast.value)(mol)
      }
  }

  @deprecated("Spark sortBy is slow, use getTopPoses instead", "Sep 29, 2016")
  override def sortByScore = {
    val res = rdd.sortBy(PosePipeline
      .parseScore(methodBroadcast.value), false)
    new PosePipeline(res, scoreMethod)
  }
  @deprecated("getTopPoses includes collapsing", "Sep 29, 2016")
  override def collapse(bestN: Int) = {
    val res = rdd.groupBy(PosePipeline.parseId)
      .flatMap(PosePipeline.collapsePoses(bestN, PosePipeline.parseScore(methodBroadcast.value)))
    new PosePipeline(res, scoreMethod)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new PosePipeline(res, scoreMethod)
  }

}
