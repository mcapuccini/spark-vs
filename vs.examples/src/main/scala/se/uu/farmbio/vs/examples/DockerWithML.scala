package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution

/**
 * @author laeeq
 */

object DockerWithML extends Logging {

  case class Arglist(
    master: String = null,
    conformersFile: String = null,
    topPosesPath: String = null,
    receptorFile: String = null,
    oeLicensePath: String = null,
    topN: Int = 30)

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("DockerWithML") {
      head("DockerWithML makes use of Machine learning for efficient Docking")
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
      arg[String]("<signature-output-file>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
      opt[String]("oeLicensePath")
        .text("path to OEChem License")
        .action((x, c) => c.copy(oeLicensePath = x))
      opt[Int]("topN")
        .text("number of top scoring poses to extract (default: 30).")
        .action((x, c) => c.copy(topN = x))
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
      .setAppName("SignatureExample")
    if (params.oeLicensePath != null) {
      conf.setExecutorEnv("OE_LICENSE", params.oeLicensePath)
    }
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    val t0 = System.currentTimeMillis
    val signatures = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures()
      .dockWithML(params.receptorFile, OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getTopPoses(params.topN)

    sc.parallelize(signatures, 1).saveAsTextFile(params.topPosesPath)
    val t1 = System.currentTimeMillis
    val elapsed = t1 - t0
    logInfo(s"DockWithML pipeline took: $elapsed millisec.")

    sc.stop()

  }

}

