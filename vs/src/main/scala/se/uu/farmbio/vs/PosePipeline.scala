package se.uu.farmbio.vs

import scala.io.Source

import org.apache.spark.rdd.RDD

import org.openscience.cdk.io.MDLV2000Reader
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import org.openscience.cdk.silent.ChemFile

import java.io.ByteArrayInputStream
import java.nio.charset.Charset

trait PoseTransforms {

  def collapse(bestN: Int): SBVSPipeline with PoseTransforms
  def sortByScore: SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with PoseTransforms

}

private[vs] class PosePipeline(override val rdd: RDD[String]) extends SBVSPipeline(rdd)
    with PoseTransforms {

  private def parseId = (pose: String) => {
    Source.fromString(pose).getLines.next
  }

  private def parseScore = (pose: String) => {
    val sdfByteArray = pose
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)

    //mols is a Java list :-(
    val it = mols.iterator
    var res: String = null

    while (it.hasNext()) {
      val mol = it.next
      res = mol.getProperty("Chemgauss4")
    }
    reader.close
    res.toDouble

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