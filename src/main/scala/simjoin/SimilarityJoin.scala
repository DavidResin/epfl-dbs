package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    
    null
  }
}

