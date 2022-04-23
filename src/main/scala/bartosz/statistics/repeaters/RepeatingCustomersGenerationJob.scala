package bartosz.statistics.repeaters

import bartosz.ingestion.customers.Customer
import bartosz.statistics.dailystats.DailyStats
import org.apache.spark.sql.SparkSession

object RepeatingCustomersGenerationJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - orders")
      .master("local[*]")
      // Dataset used only by Spark 3.0+ clients
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .enableHiveSupport()
      .getOrCreate()

    val customers = sparkSession.table("customers")
      .select(Customer.CustomerId, Customer.CustomerUniqueId)
    val customersDailyStats = sparkSession.table("customers_daily_stats")
      .select(DailyStats.CustomerId)

    val repeatersCount = Transformations.countRepeaters(customersDailyStats, customers)

    println(s"Repeaters=${repeatersCount}")
  }
}
