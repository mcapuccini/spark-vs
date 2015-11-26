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
  
  def trainICPModel( 
      calibrationSize: Int,
      numIterations: Int,
      numOfICPs: Int) : AggregatedICPClassifier
  def predict(icp: AggregatedICPClassifier) : Array[(Long, Set[Double])]

}

class ICPMLPipeline[vs] (val rdd: RDD[(Long, LabeledPoint)]) extends ICPMLTransforms {
  
  
  def trainICPModel( 
      calibrationSize: Int,
      numIterations: Int,
      numOfICPs: Int) = {
       
    //Train ICPs
    val t0 = System.currentTimeMillis
    val icps = (1 to numOfICPs).map { _ =>
      //Further Splitting the training set into calibration set and proper training set
      val (calibration, properTraining) =
        ICP.splitCalibrationAndTraining(rdd.map(indexAndLabeledSignature=>indexAndLabeledSignature._2), calibrationSize, bothClasses = true)
          
      //Train ICP
      val gbt = new GBT(properTraining.cache, numIterations)
      ICP.trainClassifier(gbt, numClasses = 2, calibration)
    }
    val t1 = System.currentTimeMillis
    //Aggregate ICPs and perform tests
    val icp = new AggregatedICPClassifier(icps)
    
    val mondrianPvAndLabels = rdd.map{case(index,p) =>
      (icp.mondrianPv(p.features), p.label)
    }
    val metrics = new BinaryClassificationICPMetrics(mondrianPvAndLabels)
    icp
  }
     
  def predict(icp:AggregatedICPClassifier) = {
    //Predicting the test set
    val predictions = rdd.map { case(index,predictionData) =>
      (index, icp.predict(predictionData.features, 0.8))
      
    }.collect()
    predictions
  }
  
  def saveAsSignatureFile(path: String) = {
    
    MLUtils.saveAsLibSVMFile(rdd.map(indexAndLabeledSignature=>indexAndLabeledSignature._2), path)
    
  }
  
}



