package se.uu.farmbio.vs

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast

import openeye.oechem.OEFormat
import openeye.oechem.OEGraphMol
import openeye.oechem.OEMol
import openeye.oechem.OEUCharArray
import openeye.oechem.oechem
import openeye.oechem.oeistream
import openeye.oechem.oemolistream
import openeye.oechem.oemolostream
import openeye.oedocking.OEDock
import openeye.oedocking.oedocking
import openeye.oemolprop.OEFilter
import openeye.oeomega.OEOmega
import openeye.oeomega.oeomegalib

private[vs] object OEChemLambdas extends Logging {

  def oeFilter(filterType: Int, format: Int,
               customType: Broadcast[Array[Byte]],
               errorLevel: Int) = (smiles: String) => {

    //Silence OEChem
    oechem.OEThrow.SetLevel(errorLevel)

    //Streams for the molecules
    val imstr = new oemolistream
    imstr.SetFormat(format)
    imstr.openstring(smiles)
    val omstr = new oemolostream
    omstr.SetFormat(format)
    omstr.openstring()

    //Create filter object
    var filter: OEFilter = null
    val typeStream = new oeistream
    var typeBuff: OEUCharArray = null
    if (customType == null) {
      filter = new OEFilter(filterType)
    } else {
      typeBuff = byteBuff2OEBuff(customType.value)
      typeStream.open(typeBuff, customType.value.length)
      filter = new OEFilter(typeStream)
    }

    //Filter molecules
    val mol = new OEGraphMol
    while (oechem.OEReadMolecule(imstr, mol))
      if (filter.call(mol))
        oechem.OEWriteMolecule(omstr, mol)

    //Free some memory    
    filter.delete()
    mol.delete()
    typeStream.delete()
    if (typeBuff != null) {
      typeBuff.delete
    }

    //Getting the results as string
    val res = omstr.GetString
    imstr.close
    imstr.delete
    omstr.close
    omstr.delete
    if (res == "") {
      List()
    } else {
      List(res)
    }

  }

  def oeOmega(maxCenters: Int, 
              maxConformers: Int,
              errorLevel: Int) = (smiles: String) => {

    //Silence OEChem
    oechem.OEThrow.SetLevel(errorLevel)

    //Streams for the molecules
    val imstr = new oemolistream
    imstr.SetFormat(OEFormat.SMI)
    imstr.openstring(smiles)
    val omstr = new oemolostream
    omstr.SetFormat(OEFormat.SDF)
    omstr.openstring

    //Generating the conformers
    val omega = new OEOmega
    if (maxConformers > 0) {
      omega.SetMaxConfs(maxConformers)
    }
    val mol = new OEMol
    while (oechem.OEReadMolecule(imstr, mol)) {
      logInfo(s"generating conformer for ${mol.GetTitle()}")
      val stereo = oeomegalib.OEFlipper(mol.GetActive(), maxCenters, true)
      while (stereo.hasNext) {
        val base = stereo.next
        val mol = new OEMol(base)
        base.delete()
        if (omega.call(mol)) {
          oechem.OEWriteMolecule(omstr, mol)
        }
        mol.delete()
      }
      stereo.delete
    }
    mol.delete
    omega.delete

    //Getting the results as string
    val res = omstr.GetString
    imstr.close
    imstr.delete()
    omstr.close
    omstr.delete()
    res

  }

  def oeDocking(bcastReceptor: Broadcast[Array[Byte]],
                method: Int,
                resolution: Int,
                errorLevel: Int) = (sdf: String) => {

    //Silence OEChem
    oechem.OEThrow.SetLevel(errorLevel)

    //Streams for the molecules
    val imstr = new oemolistream
    imstr.SetFormat(OEFormat.SDF)
    imstr.openstring(sdf)
    val omstr = new oemolostream
    omstr.SetFormat(OEFormat.SDF)
    omstr.openstring
    val irstr = new oemolistream
    irstr.SetFormat(OEFormat.OEB)
    val recBuff = byteBuff2OEBuff(bcastReceptor.value)
    irstr.openstring(recBuff, bcastReceptor.value.length)

    //Initialize OEDock
    val dock = new OEDock(method, resolution)
    val rmol = new OEGraphMol
    oechem.OEReadMolecule(irstr, rmol)
    irstr.close
    irstr.delete
    dock.Initialize(rmol)

    //Dock the chunk
    val mcmol = new OEMol
    val dockedMol = new OEGraphMol
    var size = 0
    while (oechem.OEReadMolecule(imstr, mcmol)) { // This skips bad molecules automatically
      logInfo(s"docking ${mcmol.GetTitle()}")
      dock.DockMultiConformerMolecule(dockedMol, mcmol)
      val sdtag = oedocking.OEDockMethodGetName(method)
      oedocking.OESetSDScore(dockedMol, dock, sdtag)
      dock.AnnotatePose(dockedMol)
      oechem.OEWriteMolecule(omstr, dockedMol)
      mcmol.Clear
      dockedMol.Clear
      size+=1
    }
    logDebug(s"$size molecules successfully docked")

    //Free some memory
    dock.delete
    rmol.delete
    mcmol.delete
    dockedMol.delete
    recBuff.delete

    //Getting the results as string
    val res = omstr.GetString
    imstr.close
    imstr.delete
    omstr.close
    omstr.delete

    //Divide the poses and return
    SBVSPipeline.splitSDFmolecules(res)

  }

  private def byteBuff2OEBuff(byteBuffer: Array[Byte]) = {
    var buffer = new OEUCharArray(byteBuffer.length)
    (0 to byteBuffer.length - 1).foreach { i =>
      buffer.setItem(i, byteBuffer(i))
    }
    buffer
  }

}
