package se.uu.farmbio.vs.examples

import java.io.PrintWriter

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

object Docker extends Logging {
  case class Params(
    master: String = null,
    conformersFile: String = null,
    receptorFile: String = null,
    topPosesPath: String = null,
    size: String = "30",
    sampleSize: Double = 1.0,
    collapse: Int = 0,
    posesCheckpointPath: String = null)

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Docker") {
      head("Docker: an example docking pipeline.")
      opt[String]("collapse")
        .text("number of best scoring molecules with same id returned (default: all of them).")
        .action((x, c) => c.copy(collapse = x.toInt))
      opt[String]("size")
        .text("it controls how many molecules are handled within a task (default: 30).")
        .action((x, c) => c.copy(size = x))
      opt[String]("sampleSize")
        .text("it reduces the input size to the specified fraction (default: 1.0, means no reduction). " +
          "It can be used to evaluate scalability.")
        .action((x, c) => c.copy(sampleSize = x.toDouble))
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      opt[String]("posesCheckpointPath")
        .text("path to checkpoint all of the output poses before taking the top 10 (default: null)")
        .action((x, c) => c.copy(posesCheckpointPath = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<receptor-file>")
        .required()
        .text("path to input OEB receptor file")
        .action((x, c) => c.copy(receptorFile = x))
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
      .setAppName("Docker")
      .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("se.uu.farmbio.parsers.SDFRecordReader.size", params.size)

    val t0 = System.currentTimeMillis
    var sampleRDD = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .getMolecules

    if (params.sampleSize < 1.0) { //Samples Data on the basis of sampleSize Parameter
      sampleRDD = sampleRDD.sample(false, params.sampleSize) //Does not take effect for complete set
    }

    var poses = new SBVSPipeline(sc)
      .readConformerRDDs(Seq(sampleRDD))
      .dock(params.receptorFile, OEDockMethod.Chemgauss4, OESearchResolution.Standard)
    if (params.collapse > 0) {
      poses = poses.collapse(params.collapse)
    }

    val sortedPoses = poses.sortByScore
    val t1 = System.currentTimeMillis
    if (params.posesCheckpointPath != null) {
      sortedPoses.saveAsTextFile(params.posesCheckpointPath)
    }
    val res = sortedPoses
      .getMolecules
      .take(10) //take first 10

    val elapsed = t1 - t0
    logInfo(s"pipeline took: $elapsed millisec.")

    val pw = new PrintWriter(params.topPosesPath)
    res.foreach(pw.println(_))
    pw.close

  }

}
