package se.uu.farmbio.vs

import java.io.PrintWriter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import scala.reflect.io.Path
import java.nio.file.Paths

trait ConformerTransforms {

  def dock(cppExePath: String, method: Int, resolution: Int, receptor: String): SBVSPipeline with PoseTransforms
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

  override def dock(cppExePath: String, method: Int, resolution: Int, receptor: String) = {
    sc.addFile(cppExePath)
    sc.addFile(receptor)
    val p = Paths.get(receptor)
    val pipedRDD = this.pipe(List(SparkFiles.get("dockingstd"), method.toString(), resolution.toString(), SparkFiles.get(p.getFileName().toString()))).getMolecules
    val res = pipedRDD.flatMap(SBVSPipeline.splitSDFmolecules)
    new PosePipeline(res)
  }

  override def repartition() = {
    val res = rdd.repartition(defaultParallelism)
    new ConformerPipeline(res)
  }

}