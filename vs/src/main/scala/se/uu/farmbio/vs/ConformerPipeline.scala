package se.uu.farmbio.vs

import java.io.PrintWriter
import java.nio.file.Paths
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import se.uu.farmbio.sg.SGUtils

trait ConformerTransforms {
  def dock(receptorPath: String, method: Int, resolution: Int): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms
  def generateSignatures(): ConformersWithSignsTransforms
}

object ConformerPipeline {

  //The Spark built-in pipe splits molecules line by line, we need a custom one
  private def pipeString(str: String, command: List[String]) = {

    //Start executable
    val pb = new ProcessBuilder(command.asJava)
    val proc = pb.start
    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start
    // Start a thread to feed the process input 
    new Thread("stdin writer") {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)
        out.println(str)
        out.close()
      }
    }.start
    //Return results as a single string
    Source.fromInputStream(proc.getInputStream).mkString

  }

}

private[vs] class ConformerPipeline(override val rdd: RDD[String])
    extends SBVSPipeline(rdd) with ConformerTransforms {

  override def dock(receptorPath: String, method: Int, resolution: Int) = {
    val dockingstdPath = System.getenv("DOCKING_CPP")
    sc.addFile(dockingstdPath)
    sc.addFile(receptorPath)
    val receptorFileName = Paths.get(receptorPath).getFileName.toString
    val dockingstdFileName = Paths.get(dockingstdPath).getFileName.toString
    val pipedRDD = rdd.map { sdf =>
      ConformerPipeline.pipeString(sdf,
        List(SparkFiles.get(dockingstdFileName),
          method.toString(),
          resolution.toString(),
          SparkFiles.get(receptorFileName)))
    }

    val res = pipedRDD.flatMap(SBVSPipeline.splitSDFmolecules)
    new PosePipeline(res)
  }

  def generateSignatures = {
    val molsCount = rdd.count()
    val molsWithIndex = rdd.zipWithIndex()
    val molsAfterSG = molsWithIndex.flatMap { case (mol, index) => Sdf2LibSVM.sdf2signatures(mol, index + 1, molsCount) } //Compute signatures
      .cache
    val (result, sig2ID_universe) = SGUtils.sig2ID_carryData(molsAfterSG)
    val resultAsLP: RDD[(Long, LabeledPoint)] = SGUtils.sig2LP_carryData(result)

    new ConformersWithSigns(resultAsLP)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}