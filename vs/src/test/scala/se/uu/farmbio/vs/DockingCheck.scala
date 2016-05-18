//if you want to compare that serial and spark version gives same result, use this 
//or copy only the test part to SBVSPipelineTest.scala

package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import openeye.oemolprop.OEFilterType
import se.uu.farmbio.parsers.SDFRecordReader
import se.uu.farmbio.parsers.SmilesRecordReader
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DockingCheck extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local")
    .setAppName("DockingCheck")
    .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(SDFRecordReader.SIZE_PROPERTY_NAME, "3")
  sc.hadoopConfiguration.set(SmilesRecordReader.SIZE_PROPERTY_NAME, "3")
 
  test("Docking of 1000 molecules both in Parallel and serial should be same") {

    val res = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("1000sdf.sdf").getPath)
      .dock(getClass.getResource("hiv1_protease.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules
      .collect

    val dockedMolecules = TestUtils.readSDF(getClass.getResource("1000WithDock.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader).toSet
      === dockedMolecules.map(TestUtils.removeSDFheader).toSet)
  }
 
  override def afterAll() {
    sc.stop()
  }

}