package se.uu.farmbio.vs

import org.openscience.cdk.io.MDLV2000Reader
import java.nio.charset.Charset
import org.openscience.cdk.tools.manipulator.ChemFileManipulator
import java.io.ByteArrayInputStream
import org.openscience.cdk.silent.ChemFile
import se.uu.farmbio.sg.SGUtils
import se.uu.farmbio.sg.types.SignatureRecord
import org.openscience.cdk.interfaces.IAtomContainer

/**
 * @author laeeq
 */
private[vs] object Sdf2LibSVM {

  def sdf2signatures = (sdfRecord: String) => {
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

    var res = Seq[(String,String)]()

    while (it.hasNext()) {
      //for each molecule in the record compute the signature
      val mol = it.next
      
      // Molecules and their respective Signatures
      val molAndSig = (mol.toString() , SGUtils.atom2SigRecord(mol, 1, 3).toString())
      val Sig = SGUtils.atom2SigRecord(mol, 1, 3).toString()
       
      res = res ++ Seq(molAndSig)

    }
    res //return the molecules and signatures
  }

}