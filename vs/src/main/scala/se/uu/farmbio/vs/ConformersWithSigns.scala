package se.uu.farmbio.vs

import org.apache.spark.rdd.RDD

/*trait ConformersWithSignsTransforms {
  def saveAsSignatureFile(path: String)
  

}*/

private[vs] class ConformersWithSigns(val rdd: RDD[(String, String)]) /*extends ConformersWithSignsTransforms*/ {

  def getSignatures() = rdd.map{case (mol,sign) => sign}
  //def getSignatures() = rdd

  def saveAsSignatureFile(path: String) = {
    //rdd.map{case (mol,sign) => sign}.saveAsTextFile(path)
    rdd.saveAsTextFile(path)

  }

}

