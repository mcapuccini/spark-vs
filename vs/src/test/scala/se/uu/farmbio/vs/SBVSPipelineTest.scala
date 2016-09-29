package se.uu.farmbio.vs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import openeye.oedocking.OEDockMethod
import openeye.oedocking.OESearchResolution
import openeye.oemolprop.OEFilterType

import se.uu.farmbio.parsers.SDFRecordReader
import se.uu.farmbio.parsers.SmilesRecordReader
import se.uu.farmbio.sg.SGUtils

import java.io.ByteArrayInputStream
import java.nio.charset.Charset

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class SBVSPipelineTest extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local")
    .setAppName("SBVSPipelineTest")
    .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(SDFRecordReader.SIZE_PROPERTY_NAME, "3")
  sc.hadoopConfiguration.set(SmilesRecordReader.SIZE_PROPERTY_NAME, "3")

  test("filter should filter a set of SMILES according to the provided custom filter") {

    val res = new SBVSPipeline(sc)
      .readSmilesFile(getClass.getResource("molecules.smi").getPath)
      .filter(getClass.getResourceAsStream("zinc_filter.txt"))
      .getMolecules
      .collect

    val filtered = TestUtils.readSmiles(getClass.getResource("custom_filtered.smi").getPath)
    assert(res.flatMap(TestUtils.splitSmiles).toSet === filtered.toSet)

  }

  test("filter should filter a set of SMILES according to the provided filter type") {

    val res = new SBVSPipeline(sc)
      .readSmilesFile(getClass.getResource("molecules.smi").getPath)
      .filter(OEFilterType.Lead)
      .getMolecules
      .collect

    val filtered = TestUtils.readSmiles(getClass.getResource("filtered.smi").getPath)
    assert(res.flatMap(TestUtils.splitSmiles).toSet === filtered.toSet)

  }

  test("generateConformers should generate conformers from a SMILES file according to a maxCenters parameter") {

    val res = new SBVSPipeline(sc)
      .readSmilesFile(getClass.getResource("filtered.smi").getPath)
      .generateConformers(12)
      .getMolecules
      .collect

    val filteredConformers = TestUtils.readSDF(getClass.getResource("filtered_conformers.sdf").getPath)

    val resSet = res.flatMap(TestUtils.splitSDF).map(TestUtils.removeSDFheader).toSet
    val filtTest = filteredConformers.map(TestUtils.removeSDFheader).toSet
    assert(resSet === filtTest)

  }

  test("generateConformers should generate conformers from a SMILES file according to maxCenters, maxConformers"
    + " parameters") {

    val res = new SBVSPipeline(sc)
      .readSmilesFile(getClass.getResource("filtered.smi").getPath)
      .generateConformers(2, 1)
      .getMolecules
      .collect

    val filteredConformers = TestUtils.readSDF(getClass.getResource("filtered_single.sdf").getPath)

    val resSet = res.flatMap(TestUtils.splitSDF).map(TestUtils.removeSDFheader).toSet
    val filtTest = filteredConformers.map(TestUtils.removeSDFheader).toSet
    assert(resSet === filtTest)

  }

  test("dock should dock a set of conformers to a receptor and generate the poses") {

    val res = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("conformers_with_failed_mol.sdf").getPath)
      .dock(getClass.getResource("receptor.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules
      .collect

    val dockedMolecules = TestUtils.readSDF(getClass.getResource("unsorted_poses.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader).toSet
      === dockedMolecules.map(TestUtils.removeSDFheader).toSet)

  }

  test("getTopPoses should return the topN poses") {
    val topN = 10
    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("unsorted_poses.sdf").getPath, OEDockMethod.Chemgauss4)
      .getTopPoses(topN)

    val topCollapsed = TestUtils.readSDF(getClass.getResource("top_collapsed.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader).toSet === topCollapsed.map(TestUtils.removeSDFheader).toSet)

  }

  test("Signatures are maintained(not lost) after docking") {

    val molWithSigns = new SBVSPipeline(sc)
      .readConformerFile(getClass.getResource("filtered_conformers.sdf").getPath)
      .generateSignatures()
      .getMolecules

    val signsBeforeDocking = molWithSigns.map {
      case (mol) =>
        TestUtils.parseSignature(mol)

    }.collect()

    val molWithSignsAndDockingScore = new SBVSPipeline(sc)
      .readConformerRDDs(Seq(molWithSigns))
      .dock(getClass.getResource("receptor.oeb").getPath,
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules

    val signsAfterDocking = molWithSignsAndDockingScore.map {
      case (mol) =>
        TestUtils.parseSignature(mol)
    }.collect()

    //Comparing signatures before and after docking
    assert(signsBeforeDocking.toSet()
      === signsAfterDocking.toSet())

  }

  override def afterAll() {
    sc.stop()
  }

}