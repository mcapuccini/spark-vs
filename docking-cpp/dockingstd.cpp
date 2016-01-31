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

int main(int numOfArg, char* argv[])
{
	OEGraphMol receptor;
	OEReadReceptorFile(receptor, argv[3]);
        unsigned int dockMethod = atoi(argv[1]);
	unsigned int dockResolution = atoi(argv[2]);
	OEDock dock(dockMethod, dockResolution);
	dock.Initialize(receptor);


	oemolistream imstr;
	oemolostream omstr;

	imstr.SetFormat(OEFormat::SDF);	
	omstr.SetFormat(OEFormat::SDF);	

	OEMol mcmol;

	//Scoring the molecules in SDF File
<<<<<<< HEAD
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
=======
	while (OEReadMolecule(imstr, mcmol))
		{
	           OEGraphMol dockedMol;
	           unsigned int retcode;
		   retcode = dock.DockMultiConformerMolecule(dockedMol,mcmol);
	           if (retcode==OEDockingReturnCode::Success)
		   	{	
		   		string sdtag = OEDockMethodGetName(dockMethod);
	           		OESetSDScore(dockedMol, dock, sdtag);
	           		dock.AnnotatePose(dockedMol);
	           		//Writing moles to the SDF File with Scores
	           		OEWriteMolecule(omstr, dockedMol);
			}
		   else continue;
	         }
		
  	return 0;
>>>>>>> modified dockingstd.cpp and dockingstd. Only allow succesfully docked mols. Rest are neglected. A single unsuccessful mol added to conformers_with_failed_mol.sdf for testing purpose. Tests also updated accordingly.
}
