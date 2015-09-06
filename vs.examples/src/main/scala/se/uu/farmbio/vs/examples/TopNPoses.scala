package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import openeye.oedocking.OEDockMethod
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

object TopNPoses extends Logging {
  case class Params(
    master: String = null,
    poseFile: String = null,
    topPosesPath: String = null,
    oeLicensePath: String = null
    )

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("TopNPoses") {
      head("TopNPoses: an example of getting TopN Poses.")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      opt[String]("oeLicensePath")
        .text("path to OEChem License")
        .action((x, c) => c.copy(oeLicensePath = x))
      arg[String]("<pose-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(poseFile = x))
      arg[String]("<top-poses-path>")
        .required()
        .text("path to top output poses")
        .action((x, c) => c.copy(topPosesPath = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

  }

  def run(params: Params) {
    //Init Spark
    val conf = new SparkConf()
      .setAppName("TopNPoses")
    if (params.oeLicensePath != null) {
      conf.setExecutorEnv("OE_LICENSE", params.oeLicensePath)
    }
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    val t0 = System.currentTimeMillis
     val res = new SBVSPipeline(sc)
      .readPoseFile(params.poseFile, OEDockMethod.Chemgauss4)
      .getTopPoses(30)
    val t1 = System.currentTimeMillis
    sc.parallelize(res, 1).saveAsTextFile(params.topPosesPath)
    val elapsed = t1 - t0
    logInfo(s"pipeline took: $elapsed millisec.")

  }

}
