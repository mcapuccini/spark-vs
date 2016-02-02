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
import org.openscience.cdk.interfaces.IAtomContainer
import se.uu.farmbio.sg.types.SignatureRecord

trait ConformersWithSignsTransforms {
  def saveAsSignatureFile(path: String)

}

private[vs] class ConformersWithSigns(val rdd: RDD[(String, String)])
    extends ConformersWithSignsTransforms {

  def saveAsSignatureFile(path: String) = {
    //rdd.map{case (mol,sign) => sign}.saveAsTextFile(path)
    rdd.saveAsTextFile(path)

  }

}

