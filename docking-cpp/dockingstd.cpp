#include <iostream>
#include <fstream>

#include "openeye.h"
#include "oesystem.h"
#include "oechem.h"
#include "oedocking.h"

using namespace OESystem;
using namespace OEChem;
using namespace OEDocking;
using namespace std;

//ifstream fin("/home/laeeq/Desktop/virtualscreening2/Z104301.sdf");
//#define cin fin

int main()
{
	OEGraphMol receptor;
	OEReadReceptorFile(receptor, "/home/laeeq/Desktop/virtualscreening2/hiv1_protease.oeb");

	unsigned int dockMethod = OEDockMethod::Chemgauss4;
	unsigned int dockResolution = OESearchResolution::Standard;
	OEDock dock(dockMethod, dockResolution);
	dock.Initialize(receptor);

	oemolistream imstr;
	oemolostream omstr;

	imstr.SetFormat(OEFormat::SDF);	
	omstr.SetFormat(OEFormat::SDF);	

	OEMol mcmol;

	//Scoring the molecules in SDF File
	         while (OEReadMolecule(imstr, mcmol))
	         {
	           OEGraphMol dockedMol;
	           dock.DockMultiConformerMolecule(dockedMol,mcmol);
	           string sdtag = OEDockMethodGetName(dockMethod);
	           OESetSDScore(dockedMol, dock, sdtag);
	           dock.AnnotatePose(dockedMol);
	           //Writing moles to the SDF File with Scores
	           OEWriteMolecule(omstr, dockedMol);

	         }

  return 0;
}
