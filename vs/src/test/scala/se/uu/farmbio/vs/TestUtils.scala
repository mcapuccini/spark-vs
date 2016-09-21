package se.uu.farmbio.vs

import scala.collection.mutable.ListBuffer
import scala.io.Source

object TestUtils {

  def parseSignature = (pose: String) => {
    var res: String = null
    val it = SBVSPipeline.CDKInit(pose)
    while (it.hasNext()) {
      val mol = it.next
      res = mol.getProperty("Signature")
    }
    res
  }

  def removeSDFheader(sdf: String) = {
    Source.fromString(sdf).getLines.drop(3).mkString("\n")
  }

  def readSDF(path: String) = {
    val out = new ListBuffer[String]
    var mol = ""
    for (line <- Source.fromFile(path).getLines) {
      if (line == "$$$$") {
        mol += line
        out += mol
        mol = ""
      } else {
        mol += line + "\n"
      }
    }
    out
  }

  def readSmiles(path: String) = {
    Source.fromFile(path).getLines.map(_ + "\n")
  }

  def splitSmiles(smiles: String) = {
    Source.fromString(smiles).getLines.map(_ + "\n")
  }

  def splitSDF(sdf: String) = {
    sdf.trim.split("\\$\\$\\$\\$").map(_.trim + "\n$$$$").toList
  }

}