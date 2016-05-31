//if you want to compare that serial and spark version gives same result, use this 
//or copy only the test part to SBVSPipelineTest.scala

package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import se.uu.farmbio.parsers.SDFRecordReader
import se.uu.farmbio.parsers.SmilesRecordReader

import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class DockingCheck extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("DockingCheck")
    .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(SDFRecordReader.SIZE_PROPERTY_NAME, "3")
  sc.hadoopConfiguration.set(SmilesRecordReader.SIZE_PROPERTY_NAME, "3")

  test("Docking of 1000 molecules both in Parallel and serial should be same") {

    //Parallel Execution
    val resPar = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("1000mols.sdf").getPath)
      .dock(getClass.getResource("receptor.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules
      .collect

    //Serial Execution  
    val dockingstdPath = System.getenv("DOCKING_CPP")
    val conformerFile = TestUtils.readSDF(getClass.getResource("1000mols.sdf").getPath)
    val receptorFileName = Paths.get(getClass.getResource("receptor.oeb").getPath).toString
    val dockingstdFileName = Paths.get(dockingstdPath).toString
    val resSer = conformerFile.map { sdf =>
      ConformerPipeline.pipeString(sdf,
        List(dockingstdFileName,
          OEDockMethod.Chemgauss4.toString(),
          OESearchResolution.Standard.toString(),
          receptorFileName))
    }

    assert(resPar.map(TestUtils.removeSDFheader).toSet
      === resSer.map(_.trim).filter(_.nonEmpty).map(TestUtils.removeSDFheader).toSet)

  }

  override def afterAll() {
    sc.stop()
  }

}