package se.uu.farmbio.vs

import org.openscience.cdk.io.MDLV2000Reader
import java.nio.charset.Charset
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import java.io.ByteArrayInputStream
import org.openscience.cdk.silent.ChemFile
import se.uu.farmbio.sg.SGUtils
import se.uu.farmbio.sg.types.SignatureRecordDecision

/**
 * @author laeeq
 */
private[vs] object Sdf2LibSVM {

  def sdf2signatures = (poses: String, index: Long, molCount: Long) => {
    //get SDF as input stream
    val sdfByteArray = poses
      .getBytes(Charset.forName("UTF-8"))
    val sdfIS = new ByteArrayInputStream(sdfByteArray)
    //Parse SDF
    val reader = new MDLV2000Reader(sdfIS)
    val chemFile = reader.read(new ChemFile)
    val mols = ChemFileManipulator.getAllAtomContainers(chemFile)
    //mols is a Java list :-(

    val it = mols.iterator

    var res = Seq[(Long, SignatureRecordDecision)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature
      val mol = it.next
      val label = index.toDouble match { //convert labels
        case x if x <= (molCount / 2) => 1.0
        case _                        => 0.0
      }
      
      // Signature generation with decision labels
      val sig = (index, SGUtils.atom2SigRecordDecision(mol, label.toDouble, 1, 3))
      res = res ++ Seq(sig)

    }
    res //return the results
  }

}