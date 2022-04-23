package bartosz

import org.apache.spark.sql.SparkSession

trait SparkSessionTestContext {

  lazy val sparkSession = SparkSession.builder()
    .appName("Unit test SparkSession")
    .master("local[*]")
    .getOrCreate()
}
