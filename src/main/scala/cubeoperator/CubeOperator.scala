package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //TODO Task 1

    null
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    var map:Map[List[String], Any] = Map()

    // Map step in naive algo
    dataset.getRDD().collect().foreach(tuple => {
      val reduced = tuple.getValuesMap(groupingAttributes).values.toList
      var attributes:List[List[String]] = List(List())
      reduced.foreach(x => attributes = attributes ::: List(List(x, "*")))
      val comb = generateCombinations(attributes)
      comb.foreach(x => map += (x -> tuple))
    })

    map.foreach(x => print(x))

    //TODO naive algorithm for cube computation
    null
  }

  def generateCombinations(attributes: List[List[String]]): List[List[String]] = attributes match {
    case Nil => List(Nil)
    case head::tail => for(item <- head;items <- generateCombinations(tail)) yield item::items
  }

}
