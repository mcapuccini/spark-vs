package se.uu.farmbio.vs

import se.uu.farmbio.cp.AggregatedICPClassifier
import se.uu.farmbio.cp.ICP
import se.uu.farmbio.cp.alg.SVM
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.openscience.cdk.io.SDFWriter

import java.io.StringWriter

trait ConformersWithSignsTransforms {
  def dockWithML(
    receptorPath: String,
    method: Int,
    resolution: Int,
    numOfICPs: Int = 5,
    calibrationSize: Int = 50,
    numIterations: Int = 50,
    portion: Double = 100,
    divider: Double = 1000): SBVSPipeline with PoseTransforms
}

object ConformersWithSignsPipeline extends Serializable {

  private def getLPRDD(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(LabeledPoint)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature

      val mol = it.next
      val label: String = mol.getProperty("Label")
      val doubleLabel: Double = label.toDouble
      val labeledPoint = new LabeledPoint(doubleLabel, Vectors.parse(mol.getProperty("Signature")))
      res = res ++ Seq(labeledPoint)
    }

    res //return the labeledPoint
  }

  private def getFeatureVector(poses: String) = {
    val it = SBVSPipeline.CDKInit(poses)

    var res = Seq[(Vector)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the FeatureVector
      val mol = it.next
      val Vector = Vectors.parse(mol.getProperty("Signature"))
      res = res ++ Seq(Vector)
    }

    res //return the FeatureVector
  }

  private def labelTopAndBottom(sdfRecord: String, score: Double, scoreHistogram: Array[Double]) = {
    val it = SBVSPipeline.CDKInit(sdfRecord)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    while (it.hasNext()) {
      val mol = it.next
      val label = score match { //convert labels
        case x if x >= scoreHistogram(7) && x <= scoreHistogram(10) => 1.0
        case x if x >= scoreHistogram(0) && x <= scoreHistogram(5) => 0.0
        case _ => "NAN"
      }

      if (label == 0.0 || label == 1.0) {
        mol.removeProperty("cdk:Remark")
        mol.setProperty("Label", label)
        writer.write(mol)
      }
    }
    writer.close
    strWriter.toString() //return the molecule  
  }

}

private[vs] class ConformersWithSignsPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformersWithSignsTransforms {

  override def dockWithML(
    receptorPath: String,
    method: Int,
    resolution: Int,

    //Default values are given in the trait See above
    numOfICPs: Int,
    calibrationSize: Int,
    numIterations: Int,
    portion: Double,
    divider: Double) = {

    //initializations
    var divisor: Double = divider
    var poses: RDD[String] = null
    var dsTrain: RDD[String] = null
    var dsOne: RDD[(String)] = null
    var ds: RDD[String] = rdd.cache()
    var eff: Double = 0.0
    var counter: Int = 1

    do {

      //Step 1
      //Get a sample of the data
      val dsInit = ds.sample(false, portion / divisor, 1234)

      if (divisor > portion) {
        divisor = divisor - portion
      }
      //Step 2
      //Subtract the sampled molecules from main dataset
      ds = ds.subtract(dsInit)

      //Step 3
      //Docking the sampled dataset
      val dsDock = ConformerPipeline
        .getDockingRDD(receptorPath, method, resolution, false, sc, dsInit)
        //Removing empty molecules caused by oechem optimization problem
        .map(_.trim).filter(_.nonEmpty).cache()

      //Step 4
      //Keeping processed poses
      if (poses == null)
        poses = dsDock
      else
        poses = poses.union(dsDock)

      //Step 5 and 6 Computing dsTopAndBottom
      val parseScoreRDD = dsDock.map(PosePipeline.parseScore(method))
      val parseScoreHistogram = parseScoreRDD.histogram(10)

      val dsTopAndBottom = dsDock.map {
        case (mol) =>
          val score = PosePipeline.parseScore(method)(mol)
          ConformersWithSignsPipeline.labelTopAndBottom(mol, score, parseScoreHistogram._1)
      }.map(_.trim).filter(_.nonEmpty)

      //Step 7 Union dsTrain and dsTopAndBottom
      if (dsTrain == null)
        dsTrain = dsTopAndBottom
      else
        dsTrain = dsTrain.union(dsTopAndBottom)

      //Converting SDF training set to LabeledPoint required for conformal prediction
      val lpDsTrain = dsTrain.flatMap {
        sdfmol => ConformersWithSignsPipeline.getLPRDD(sdfmol)
      }

      //Step 8 Training
      //Train icps

      val icps = (1 to numOfICPs).map { _ =>
        val (calibration, properTraining) =
          ICP.calibrationSplit(lpDsTrain, calibrationSize)
        //Train ICP
        val svm = new SVM(properTraining.cache, numIterations)
        ICP.trainClassifier(svm, numClasses = 2, calibration)
      }

      //SVM based Aggregated ICP Classifier (our model)
      val icp = new AggregatedICPClassifier(icps)

      //Converting SDF main dataset (ds) to feature vector required for conformal prediction
      //We also need to keep intact the poses so at the end we know
      //which molecules are predicted as bad and remove them from main set

      val fvDs = ds.flatMap {
        sdfmol =>
          ConformersWithSignsPipeline.getFeatureVector(sdfmol)
            .map { case (vector) => (sdfmol, vector) }
      }

      //Step 9 Prediction using our model
      val predictions = fvDs.map {
        case (sdfmol, predictionData) => (sdfmol, icp.predict(predictionData, 0.2))
      }

      val dsZero: RDD[(String)] = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(0.0)) }
        .map { case (sdfmol, prediction) => sdfmol }
      dsOne = predictions
        .filter { case (sdfmol, prediction) => (prediction == Set(1.0)) }
        .map { case (sdfmol, prediction) => sdfmol }

      //Step 10 Subtracting {0} moles from dataset
      ds = ds.subtract(dsZero).cache

      //Computing efficiency for stopping
      val totalCount = sc.accumulator(0.0)
      val singletonCount = sc.accumulator(0.0)

      predictions.foreach {
        case (sdfmol, prediction) =>
          if (prediction.size == 1) {
            singletonCount += 1.0
          }
          totalCount += 1.0
      }

      eff = singletonCount.value / totalCount.value
      counter = counter + 1
    } while (eff < 0.8 || counter < 4)

    //Docking rest of the dsOne mols
    val dsDockOne = ConformerPipeline.getDockingRDD(receptorPath, method, resolution, false, sc, dsOne)
      //Removing empty molecules caused by oechem optimization problem
      .map(_.trim).filter(_.nonEmpty).cache()

    //Keeping rest of processed poses i.e. dsOne mol poses
    if (poses == null)
      poses = dsDockOne
    else
      poses = poses.union(dsDockOne)

    new PosePipeline(poses, method)

  }

}