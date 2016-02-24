package se.uu.farmbio.vs

import java.io.PrintWriter
import java.nio.file.Paths
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import se.uu.farmbio.sg.SGUtils
import org.openscience.cdk.interfaces.IAtomContainer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext._

trait ConformerTransforms {
  def dock(receptorPath: String, method: Int, resolution: Int): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms
  def generateSignatures(): SBVSPipeline with ConformersWithSignsTransforms
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
  /*
  override def generateSignatures = {
    val molsRDD = rdd.flatMap { mol => Sdf2LibSVM.SdfStringToIAtomContainer(mol) }
    val molsWithID = molsRDD.zipWithUniqueId()
    val molsWithCarryAndLabels = molsWithID.map{case(mol,id)=>(id, 0.0, mol)}
    val (lps, mapping) = SGUtils.atoms2LP_UpdateSignMapCarryData(molsWithCarryAndLabels, null, 1, 3)
    val idAndParseVector = lps.map { case (id, lp) => (id, lp.features.toSparse.toString()) }
    val idAndMols = molsWithID.map{x=>x.swap}
  
    
    new ConformersWithSignsPipeline(idAndMols.map(x=>x.toString()))
  }
  */

  override def generateSignatures = {
    //Give ids to SDFMols. Ids used later for joining signature vector with mol.
    val idAndSdfMols = rdd.flatMap { mol => SBVSPipeline.splitSDFmolecules(mol) }.zipWithUniqueId().map { x => x.swap } 
    //Converts string to IAtomContainer which is required for SG library
    val molsRDD = idAndSdfMols.flatMapValues { case (mol) => Sdf2LibSVM.SdfStringToIAtomContainer(mol) }
    //Follwing steps perform Signature generation
    val molsWithCarryAndLabels = molsRDD.map { case (id, mol) => (id, 0.0, mol) }
    val (lps, mapping) = SGUtils.atoms2LP_UpdateSignMapCarryData(molsWithCarryAndLabels, null, 1, 3)
    //Give id for ginature vector.
    val idAndParseVector = lps.map { case (id, lp) => (id, lp.features.toSparse.toString()) }
    //Joining mol and vector based on id.
    val joinedRDD = idAndSdfMols.join(idAndParseVector)
    //We don't need ids anymore. 
    val res = joinedRDD.map { case (id, (mol, sign)) => (mol, sign).toString() }
    new ConformersWithSignsPipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}