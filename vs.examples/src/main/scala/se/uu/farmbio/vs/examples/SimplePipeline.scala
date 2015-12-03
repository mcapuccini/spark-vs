package se.uu.farmbio.vs.examples

import java.io.FileInputStream
import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import openeye.oemolprop.OEFilterType
import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

object SimplePipeline {

  case class Params(
    master: String = null,
    cppExeFile: String = null,
    topPosesPath: String = null,
    smilesFile: String = null,
    conformersPath: String = null)

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("SimplePipeline") {
      head("SimplePipeline: a simple SBVS pipeline.")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<CPP-EXE-file>")
        .required()
        .text("path to input cpp Executable file")
        .action((x, c) => c.copy(cppExeFile = x))
      arg[String]("<input-smiles-file>")
        .required()
        .text("path to input SMILES file")
        .action((x, c) => c.copy(smilesFile = x))
      arg[String]("<out-conformers-path>")
        .required()
        .text("path to output SDF conformes")
        .action((x, c) => c.copy(conformersPath = x))
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
      .setAppName("SimplePipeline")
      .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("se.uu.farmbio.parsers.SmilesRecordReader.size", "150")

    val res = new SBVSPipeline(sc)
      .readSmilesFile(params.smilesFile)
      .filter(OEFilterType.Lead)
      .generateConformers(0, 1) //generate 1 conformer per SMILES
      .saveAsTextFile(params.conformersPath)
      .dock(params.cppExeFile, OEDockMethod.Chemgauss4,
        OESearchResolution.Standard,"data/receptor.oeb")
      .sortByScore
      .getMolecules
      .take(10) //take first 10

    val pw = new PrintWriter(params.topPosesPath)
    res.foreach(pw.println(_))
    pw.close

  }

}