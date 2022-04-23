package bartosz.statistics.topcustomers

import org.apache.spark.sql.SparkSession

object TopCustomersGenerationJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - orders")
      .master("local[*]")
      // Dataset used only by Spark 3.0+ clients
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._

    val topCustomers = Transformations.getTopCustomers(sparkSession.table("customers_daily_stats"))

    topCustomers.limit(40)
      .show(40, false)
  }

}
