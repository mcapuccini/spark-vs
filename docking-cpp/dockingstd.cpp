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
	unsigned int retcode;
	string sdData;	
	//Scoring the molecules in SDF File
	while (OEReadMolecule(imstr, mcmol))
		{
	           
		   OEGraphMol dockedMol;
		   retcode = dock.DockMultiConformerMolecule(dockedMol,mcmol);
	           if (retcode==OEDockingReturnCode::Success)
		   	{	
		   		string sdtag = OEDockMethodGetName(dockMethod);
				OEGraphMol gfMol;
				//copy sd tag data i.e. signatures from mcmol to dockedMol if it exists
				for(OEIter<OEConfBase> conf = mcmol.GetConfs(); conf; ++conf)
		                        if (OEHasSDData(conf,"Signature"))
                	                {
						sdData = OEGetSDData(conf,"Signature");
						OESetSDData(dockedMol,"Signature", sdData);
					}
				OESetSDScore(dockedMol, dock, sdtag);
	           		dock.AnnotatePose(dockedMol);
	           		//Writing moles to the SDF File with Scores
	           		OEWriteMolecule(omstr, dockedMol);
			}
	         }


  	return 0;
}
