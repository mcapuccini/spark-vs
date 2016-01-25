package se.uu.farmbio.vs

import java.io.PrintWriter
import java.nio.file.Paths

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import se.uu.farmbio.cp.AggregatedICPClassifier
import se.uu.farmbio.cp.BinaryClassificationICPMetrics
import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.ICPClassifierModel
import se.uu.farmbio.cp.alg.GBT

trait ConformersWithSignsTransforms {
  def saveAsSignatureFile(path: String)

}

private[vs] class ConformersWithSigns(val rdd: RDD[(Long, LabeledPoint)])
    extends ConformersWithSignsTransforms {

  def saveAsSignatureFile(path: String) = {
    MLUtils.saveAsLibSVMFile(rdd.map(indexAndLabeledSignature => indexAndLabeledSignature._2), path)
  }

}

