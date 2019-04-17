package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Executor {
  def execute_Q1(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 1)
    // For example, for Q1, params(0) is the interval from the where close
  }

  def execute_Q3(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }
}
