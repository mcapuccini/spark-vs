package se.uu.farmbio.vs.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import se.uu.farmbio.vs.SBVSPipeline
import openeye.oemolprop.OEFilterType
import scopt.OptionParser
import org.apache.spark.Logging

object ZincLikeStandardizer extends Logging {

  case class Params(
    master: String = null,
    smilesFile: String = null,
    conformersPath: String = null,
    size: String = "80")

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("ZincLikeStandardizer") {
      head("ZincLikeStandardizer: a zinc like standardization pipeline.")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      opt[String]("size")
        .text("it controls how many molecules are handled within a task (default: 80).")
        .action((x, c) => c.copy(size = x))
      arg[String]("<input-smiles-file>")
        .required()
        .text("path to input SMILES file")
        .action((x, c) => c.copy(smilesFile = x))
      arg[String]("<out-conformers-path>")
        .required()
        .text("path to output SDF conformes")
        .action((x, c) => c.copy(conformersPath = x))
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
      .setAppName("ZincLikeStandardizer")
      .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
    if (params.master != null) {
      conf.setMaster(params.master)
    }

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("se.uu.farmbio.parsers.SmilesRecordReader.size", params.size)

    val zincDefaultFilter = getClass
      .getResourceAsStream("zinc_primary_rules.txt")

    val t0 = System.currentTimeMillis
    
    new SBVSPipeline(sc)
      .readSmilesFile(params.smilesFile)
      .filter(zincDefaultFilter)
      .filter(OEFilterType.Lead)
      .repartition
      .generateConformers(2, 1) //max 2 stereocenters, max 1 conformer 
      .saveAsTextFile(params.conformersPath)
      
    val t1 = System.currentTimeMillis
    val elapsed = t1 - t0
    logInfo(s"pipeline took: $elapsed millisec.")

  }

}