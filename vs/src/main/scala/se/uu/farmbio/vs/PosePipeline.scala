package se.uu.farmbio.vs

import scala.io.Source

import org.apache.spark.rdd.RDD
import uu.farmbio.sg.SGUtils
import org.apache.spark.mllib.regression.LabeledPoint

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition : SBVSPipeline with PoseTransforms
  def generateSignaturesWithDocking() : ICPMLTransforms
}

class PosePipeline[vs] (override val rdd: RDD[String]) extends SBVSPipeline(rdd)
    with PoseTransforms {
  
  def generateSignaturesWithDocking = {
      val molsCount = rdd.count()
      val molsWithIndex = rdd.zipWithIndex()
      val molsAfterSG = molsWithIndex.flatMap{case(mol, index) => Sdf2LibSVM.sdf2signatures(mol,index + 1,molsCount)} //Compute signatures
      .cache
      val (result, sig2ID_universe) = SGUtils.sig2ID_carryData(molsAfterSG)
      val resultAsLP : RDD[(Long, LabeledPoint)] = SGUtils.sig2LP_carryData(result, sc);
      
      new ICPMLPipeline(resultAsLP)
   }

    private def parseId = (pose: String) => {
      Source.fromString(pose).getLines.next
    }

    private def parseScore = (pose: String) => {
      val lines = Source.fromString(pose).getLines.toArray
      lines(lines.length - 3).toDouble 
    }

    private def collapsePoses(bestN: Int, parseScore: String => Double) = (record: (String, Iterable[String])) => {
      record._2.toList.sortBy(parseScore).reverse.take(bestN)
    } 

    override def sortByScore = {
      val res = rdd.sortBy(parseScore, false)
      new PosePipeline(res)
    }

    override def collapse(bestN: Int) = {
      val res = rdd.groupBy(parseId)
        .flatMap(collapsePoses(bestN, parseScore))
      new PosePipeline(res)
    }

    override def repartition() = {
      val res = rdd.repartition(defaultParallelism)
      new PosePipeline(res)
    }

  }