package se.uu.farmbio.vs.examples

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scopt.OptionParser
import se.uu.farmbio.vs.SBVSPipeline

/**
 * @author laeeq
 */

case class Arglist(
  master: String = null,
  conformersFile: String = null,
  signatureOutputFile: String = null)

object SignatureExample extends Logging {

  def main(args: Array[String]) {
    val defaultParams = Arglist()
    val parser = new OptionParser[Arglist]("Signature-Example") {
      head("Signature-Example: Shows that signature generation working properly")
      opt[String]("master")
        .text("spark master")
        .action((x, c) => c.copy(master = x))
      arg[String]("<conformers-file>")
        .required()
        .text("path to input SDF conformers file")
        .action((x, c) => c.copy(conformersFile = x))
      arg[String]("<Signature-Output-File>")
        .required()
        .text("path to Output Signature file")
        .action((x, c) => c.copy(signatureOutputFile = x))
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

    if (params.master != null) {
      conf.setMaster(params.master)
    }
    val sc = new SparkContext(conf)
    val signatures = new SBVSPipeline(sc)
      .readConformerFile(params.conformersFile)
      .generateSignatures()
      .saveAsSignatureFile(params.signatureOutputFile)

    sc.stop()

  }

}

