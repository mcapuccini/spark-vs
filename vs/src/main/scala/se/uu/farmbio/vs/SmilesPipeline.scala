package se.uu.farmbio.vs

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD

import openeye.oechem.OEFormat

trait SmilesTransforms {
  
    def filter(filterType: Int): SBVSPipeline with SmilesTransforms
    def filter(customFilter: InputStream): SBVSPipeline with SmilesTransforms
    def generateConformers(maxCenters: Int): SBVSPipeline with ConformerTransforms
    def generateConformers(maxCenters: Int, maxConformers: Int): SBVSPipeline with ConformerTransforms
    def repartition : SBVSPipeline with SmilesTransforms
  
}

class SmilesPipeline[vs] (override val rdd: RDD[String]) 
extends SBVSPipeline(rdd) with SmilesTransforms {

    override def filter(filterType: Int) = {
      val res = rdd.flatMap(OEChemLambdas.oeFilter(filterType, OEFormat.SMI, null,oeErrorLevel))
      new SmilesPipeline(res)
    }
    
    override def filter(customFilter: InputStream) = {
      val cutomFilter = IOUtils.toByteArray(customFilter)
      val bcastFilter = sc.broadcast(cutomFilter)
      val res = rdd.flatMap(OEChemLambdas.oeFilter(0, OEFormat.SMI, bcastFilter,oeErrorLevel))
      new SmilesPipeline(res)
    }

    override def generateConformers(maxCenters: Int) = {
      val res = rdd.map(OEChemLambdas.oeOmega(maxCenters,0,oeErrorLevel)) //0 means no max conformers limit
      new ConformerPipeline(res)
    }
    
    override def generateConformers(maxCenters: Int, maxConformers: Int) = {
      val res = rdd.map(OEChemLambdas.oeOmega(maxCenters,maxConformers,oeErrorLevel)) 
      new ConformerPipeline(res)
    }

    override def repartition() = {
      val res = rdd.repartition(defaultParallelism)
      new SmilesPipeline(res)
    }

  }