package se.uu.farmbio.vs

import scala.io.Source

import org.apache.spark.rdd.RDD

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition : SBVSPipeline with PoseTransforms
  
}

class PosePipeline[vs] (override val rdd: RDD[String]) extends SBVSPipeline(rdd)
    with PoseTransforms {

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