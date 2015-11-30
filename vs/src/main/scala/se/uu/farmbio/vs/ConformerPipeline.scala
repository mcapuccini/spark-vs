package se.uu.farmbio.vs

import java.io.InputStream
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.rdd.RDD
import scala.io.Source
import java.io.PrintWriter
import scala.collection.JavaConverters._

trait ConformerTransforms {

  def dock(receptor: InputStream, method: Int, resolution: Int): SBVSPipeline with PoseTransforms
  def repartition: SBVSPipeline with ConformerTransforms

}

private[vs] class ConformerPipeline(override val rdd: RDD[String])
  extends SBVSPipeline(rdd) with ConformerTransforms {

  //The Spark built-in pipe splits molecules line by line, we need a custom one
  def pipe(command: List[String]) = {

    val res = rdd.map { sdf =>
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
          out.println(sdf)
          out.close()
        }
      }.start
      //Return results as a single string
      Source.fromInputStream(proc.getInputStream).mkString
    }
    
    new ConformerPipeline(res)

  }

  override def dock(receptor: InputStream, method: Int, resolution: Int) = {
    //    val receptorBytes = IOUtils.toByteArray(receptor)
    //    val bcastReceptor = sc.broadcast(receptorBytes)
    //    val res = rdd.flatMap(OEChemLambdas.oeDocking(bcastReceptor, method, resolution, oeErrorLevel))
    //    new PosePipeline(res)
    val cppExePath = "/home/laeeq/Desktop/spark-vs/docking-cpp/dockingstd"
    val pipedRDD = rdd.pipe(cppExePath)

    val res = pipedRDD.flatMap(SBVSPipeline.splitSDFmolecules)

    /*
    val res = pipedRDD.collect()
       
     
    val string = res.mkString("\n")
    val res2 = SBVSPipeline.splitSDFmolecules(string)
    val cppRDD = sc.makeRDD(res2)    
    */

    new PosePipeline(res)

    //throw new NotImplementedException("Needs to be reimplemented due to memory issue")
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}