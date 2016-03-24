package se.uu.farmbio.vs

import java.io.PrintWriter
import java.io.StringWriter
import org.openscience.cdk.io.SDFWriter
import java.nio.file.Paths
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import se.uu.farmbio.sg.SGUtils
import org.openscience.cdk.interfaces.IAtomContainer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext._
import org.openscience.cdk.io.MDLV2000Reader
import java.nio.charset.Charset
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import java.io.ByteArrayInputStream
import org.openscience.cdk.silent.ChemFile

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
  
  private def SdfStringToIAtomContainer = (sdfRecord: String) => {
    //get SDF as input stream
    val sdfByteArray = sdfRecord
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
       
    //mols is a Java list :-(

    val it = mols.iterator

    var res = Seq[(IAtomContainer)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature
     
      val mol = it.next
                 
      res = res ++ Seq(mol)
    }

    res //return the molecule
  }
  
  private def writeSignature = (sdfRecord: String, signature: String) => {
    //get SDF as input stream
    val sdfByteArray = sdfRecord
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
    val strWriter = new StringWriter()
    val writer = new SDFWriter(strWriter)
    
    //mols is a Java list :-(
    val it = mols.iterator
  
    while (it.hasNext()) {
      val mol = it.next
      mol.setProperty("Signature", signature)
      mol.removeProperty("cdk:Remark")
      writer.write(mol)
    }
    writer.close
    reader.close
    strWriter.toString() //return the molecule  
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
   
   override def generateSignatures = {
    //Give ids to SDFMols. Ids used later for joining signature vector with mol.
    val idAndSdfMols = rdd.flatMap { mol => SBVSPipeline.splitSDFmolecules(mol) }.zipWithUniqueId().map { x => x.swap }
    //Converts string to IAtomContainer which is required for SG library
    val molsRDD = idAndSdfMols.flatMapValues { case (mol) => ConformerPipeline.SdfStringToIAtomContainer(mol) }
    //Following steps perform Signature generation
    val molsWithCarryAndLabels = molsRDD.map { case (id, mol) => (id, 0.0, mol) }
    val (lps, mapping) = SGUtils.atoms2LP_UpdateSignMapCarryData(molsWithCarryAndLabels, null, 1, 3)
    //Give id for signature vector.
    val idAndParseVector = lps.map { case (id, lp) => (id, lp.features.toSparse.toString()) }
    //Joining mol and vector based on id.
    val joinedRDD = idAndSdfMols.join(idAndParseVector)
    //We don't need Ids anymore. 
    val res = joinedRDD.map { case (id, (mol, sign)) => ConformerPipeline.writeSignature(mol,sign) }
    new ConformersWithSignsPipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}