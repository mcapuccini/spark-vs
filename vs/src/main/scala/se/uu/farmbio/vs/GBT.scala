package se.uu.farmbio.vs

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.rdd.RDD

import se.uu.farmbio.cp.UnderlyingAlgorithm

//Define a GBTs UnderlyingAlgorithm
class GBT(
  override val input: RDD[LabeledPoint],
  private val numIterations: Int)
  extends UnderlyingAlgorithm(input) {

  override def trainingProcedure(input: RDD[LabeledPoint]) = {
    //Configuration
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = numIterations
    boostingStrategy.treeStrategy.maxDepth = 5
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    boostingStrategy.loss = LogLoss
    //Training
    val remappedInput = input.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
    val model = new GradientBoostedTrees(boostingStrategy)
      .run(input = remappedInput)
    model.predict
  }

  override def nonConformityMeasure(newSample: LabeledPoint) = {
    val score = predictor(newSample.features)
    if (newSample.label == 1.0) {
      -score
    } else {
      score
    }
  }
}