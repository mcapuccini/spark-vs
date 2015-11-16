package se.uu.farmbio.vs

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import se.uu.farmbio.parsers.SDFInputFormat
import se.uu.farmbio.parsers.SmilesInputFormat
import org.apache.spark.Logging
import openeye.oechem.OEErrorLevel

private object SBVSPipeline {

  def splitSDFmolecules(molecules: String) = {
    molecules.trim.split("\\$\\$\\$\\$").map(_.trim + "\n\n$$$$").toList
  }

}

class SBVSPipeline (protected val rdd: RDD[String]) extends Logging {
  
  def this(sc: SparkContext) = {
    this(sc.emptyRDD[String])
  }

  protected val sc = rdd.context
  protected val defaultParallelism = sc.getConf.get("spark.default.parallelism", "2").toInt
  protected val oeErrorLevel =
    sc.getConf.get("oechem.error.level", OEErrorLevel.Error.toString).toInt
  logDebug(s"OEChem error level is: $oeErrorLevel")  
  
  def getMolecules = rdd

  def readSmilesRDDs(smiles: Seq[RDD[String]]): SBVSPipeline with SmilesTransforms = {
    new SmilesPipeline(sc.union(smiles))
  }

  def readConformerRDDs(conformers: Seq[RDD[String]]): SBVSPipeline with ConformerTransforms = {
    new ConformerPipeline(sc.union(conformers))
  }

  def readPoseRDDs(poses: Seq[RDD[String]]): SBVSPipeline with PoseTransforms = {
    new PosePipeline(sc.union(poses))
  }

  def readSmilesFile(path: String): SBVSPipeline with SmilesTransforms = {
    val rdd = sc.hadoopFile[LongWritable, Text, SmilesInputFormat](path, defaultParallelism)
      .map(_._2.toString) //convert to string RDD
    new SmilesPipeline(rdd)
  }

  def readConformerFile(path: String): SBVSPipeline with ConformerTransforms = {
    val rdd = sc.hadoopFile[LongWritable, Text, SDFInputFormat](path, defaultParallelism)
      .map(_._2.toString) //convert to string RDD
    new ConformerPipeline(rdd)
  }

  def readPoseFile(path: String): SBVSPipeline with PoseTransforms = {
    val rdd = sc.hadoopFile[LongWritable, Text, SDFInputFormat](path, defaultParallelism)
      .flatMap(mol => SBVSPipeline.splitSDFmolecules(mol._2.toString)) //convert to string RDD and split
    new PosePipeline(rdd)
  }

  def saveAsTextFile(path: String): this.type = {
    rdd.saveAsTextFile(path)
    this
  }

}

