package se.uu.farmbio.vs

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import se.uu.farmbio.cp.AggregatedICPClassifier
import se.uu.farmbio.cp.BinaryClassificationICPMetrics
import se.uu.farmbio.cp.ICP

import se.uu.farmbio.cp.ICPClassifierModel

/**
 * @author laeeq
 */

trait ICPMLTransforms {
  
  def saveAsSignatureFile(path:String)

}

class ICPMLPipeline[vs] (val rdd: RDD[(Long, LabeledPoint)]) extends ICPMLTransforms {
 
  def saveAsSignatureFile(path: String) = {
    
    MLUtils.saveAsLibSVMFile(rdd.map(indexAndLabeledSignature=>indexAndLabeledSignature._2), path)
    
  }
  
}



