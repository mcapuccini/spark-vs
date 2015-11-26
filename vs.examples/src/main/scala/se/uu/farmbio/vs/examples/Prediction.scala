package se.uu.farmbio.vs.examples

import org.openscience.cdk.io.MDLV2000Reader
import org.apache.spark.SparkConf
import se.uu.farmbio.vs.SBVSPipeline
import java.nio.charset.Charset
import uu.farmbio.sg.SGUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import java.io.ByteArrayInputStream
import org.apache.spark.Logging
import scopt.OptionParser
import org.openscience.cdk.silent.ChemFile
import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import java.io.FileInputStream
import java.io.PrintWriter
import se.uu.farmbio.vs.ICPMLPipeline

import java.io.File



/**
 * @author laeeq
 */

case class Arglist(
  master: String = null,
  conformersFile: String = null,
  receptorFile: String = null,
  predictionOutputFile: String = null,
  
  calibrationSize: Int = 0,
  numIterations: Int = 0,
  numOfICPs: Int = 0,
  size: String = "30",
  DSinit: Double = 0.5,
  k:  Double = 0.5
 )

object Prediction extends Logging {

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Prediction") {
      head("Prediciton: Training ICPML and Predicting")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<receptor-file>")
        .required()
        .text("path to input OEB receptor file")
        .action((x, c) => c.copy(receptorFile = x))
      arg[String]("<Signature-Output-File>")
        .required()
        .text("path to Output Prediction file")
        .action((x, c) => c.copy(predictionOutputFile = x))
      opt[Int]("calibrationSize")
        .required()
        .text(s"size of calibration set (for each class)")
        .action((x, c) => c.copy(calibrationSize = x))
      opt[Int]("numIterations")
        .required()
        .text(s"number of GBT iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Int]("numOfICPs")
        .required()
        .text(s"number of ICPs to train")
        .action((x, c) => c.copy(numOfICPs = x))
      opt[String]("size")
        .text("It controls how many molecules are handled within a task (default: 30).")
        .action((x, c) => c.copy(size = x))
      opt[Double]("DSinit")
        .required()
        .text("Fraction of the molecular library to dock initially.")
        .action((x, c) => c.copy(DSinit = x))
      opt[Double]("k")
        .text("Fraction of the molecular library to predict using ICPML Model.")
        .action((x, c) => c.copy(k = x))
      }
    
    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
    System.exit(0)
  }

  def run(params: Arglist) {

    //Init Spark
    val conf = new SparkConf()
      .setAppName("SignatureCalculator")
     
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("se.uu.farmbio.vs.io.SDFRecordReader.size", params.size)

    val receptorStream = new FileInputStream(params.receptorFile)

    //Read input file


    val Array(dsInit,remaining) = new SBVSPipeline(sc)
    .readConformerFile(params.conformersFile)
    .getMolecules.randomSplit(Array(params.DSinit,1-params.DSinit), 1234)  //Draw initial molecules to train initial ML model
    
    val cachedRem = remaining.cache()
    
    
    val train = new SBVSPipeline(sc).readConformerRDDs(Seq(dsInit.cache))
    .dock(receptorStream, OEDockMethod.Chemgauss4, OESearchResolution.Standard)
    //.collapse(1) //collapse poses with same id to the one with best score
    .sortByScore
    .generateSignatures()
    //.saveAsSignatureFile("processed/test.txt")
    .trainICPModel(params.calibrationSize, params.numIterations, params.numOfICPs)
       
    val kmolecules = new SBVSPipeline(sc)
    .readConformerRDDs(Seq(remaining))
//    .readConformerFile("data/remainingLib.sdf")
    //.randomSplit(params.k, 1234) //Read kmolecules from remainingLib file and saves the rest in remainingLib.sdf
       
    val kmolsign = kmolecules.generateSignatures()
    
    //.saveAsTextFile("processed/kmolecules.sdf") //saves already processed kmolecules in sdf format
    //.generateSignatures()
    
    //kmolecules.saveAsSignatureFile("processed/kmolecules.txt") //saves already processed kmolecules signatures
    val molsWithIndex = kmolecules.getMolecules.zipWithIndex()
    
    val predictionWithIndex = kmolsign.predict(train)
        
    //The number of k molecules
    println("The number of molecules are " + molsWithIndex.count())
    //Saving Predictions to PredictionFile  
    val pw = new PrintWriter(params.predictionOutputFile)
    pw.println(predictionWithIndex.deep)
    pw.close
  
    sc.stop()
    
   
  }
  
}

