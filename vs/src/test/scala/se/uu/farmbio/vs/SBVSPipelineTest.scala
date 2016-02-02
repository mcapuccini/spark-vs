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
class SBVSPipelineTest extends FunSuite with BeforeAndAfterAll {

  //Init Spark
  private val conf = new SparkConf()
    .setMaster("local")
    .setAppName("SBVSPipelineTest")
    .setExecutorEnv("OE_LICENSE", System.getenv("OE_LICENSE"))
  private val sc = new SparkContext(conf)
  sc.hadoopConfiguration.set(SDFRecordReader.SIZE_PROPERTY_NAME, "3")
  sc.hadoopConfiguration.set(SmilesRecordReader.SIZE_PROPERTY_NAME, "3")

  test("sortByScore should sort a set of poses by score") {

    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("filtered_collapsed.sdf").getPath)
      .sortByScore
      .getMolecules
      .collect

    val sortedPoses = TestUtils.readSDF(getClass.getResource("filtered_collapsed_sorted.sdf").getPath)
    assert(res === sortedPoses)

  }

  test("collapse should collapse poses with same id to n with highest score") {

    val n = 2

    val res = new SBVSPipeline(sc)
      .readPoseFile(getClass.getResource("filtered_poses.sdf").getPath)
      .collapse(n)
      .getMolecules
      .collect

    val filteredCollapsed = TestUtils.readSDF(getClass.getResource("filtered_collapsed.sdf").getPath)
    assert(res.toSet === filteredCollapsed.toSet)

  }

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
<<<<<<< HEAD
      .dock(getClass.getResource("receptor.oeb").getPath, 
=======
      .dock(getClass.getResource("receptor.oeb").getPath,
>>>>>>> d2778f9700c864c31983706c8dcdcc74996e4698
        OEDockMethod.Chemgauss4, OESearchResolution.Standard)
      .getMolecules
      .collect

    val filteredPoses = TestUtils.readSDF(getClass.getResource("new_pose_file.sdf").getPath)
    assert(res.map(TestUtils.removeSDFheader).toSet
      === filteredPoses.map(TestUtils.removeSDFheader).toSet)

  }

  override def afterAll() {
    sc.stop()
  }

}