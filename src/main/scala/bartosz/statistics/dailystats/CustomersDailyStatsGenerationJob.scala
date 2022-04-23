package bartosz.statistics.dailystats

import bartosz.model.PartitionData
import bartosz.statistics.dailystats.Transformations.{aggregatePreparedOrders, prepareOrdersForAggregation}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object CustomersDailyStatsGenerationJob {

  def main(args: Array[String]): Unit = {
    val partitionData = PartitionData.fromDateString(args(0))
    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - orders")
      .master("local[*]")
      .enableHiveSupport()
      // Dataset used only by Spark 3.0+ clients
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .getOrCreate()
    import sparkSession.implicits._

    val preparedOrdersForAggregation = prepareOrdersForAggregation(sparkSession.table("orders"),
      partitionData)

    val aggregatedOrdersPerCustomer = aggregatePreparedOrders(preparedOrdersForAggregation)

    aggregatedOrdersPerCustomer
      .withColumn(PartitionData.YearColumn, functions.lit(partitionData.year))
      .withColumn(PartitionData.MonthColumn, functions.lit(partitionData.month))
      .withColumn(PartitionData.DayColumn, functions.lit(partitionData.day))
      .write.mode(SaveMode.Append)
      .format("parquet")
      .partitionBy(PartitionData.YearColumn, PartitionData.MonthColumn, PartitionData.DayColumn)
      .saveAsTable("customers_daily_stats")

  }

}


object CustomersDailyStatsGenerationJobLocalIngestion {

  def main(args: Array[String]): Unit = {
    val backfillStartDate = LocalDate.of(2016, 9, 4)
    val backfillEndDate = LocalDate.of(2018, 10, 17)
    var backfilledDate = backfillStartDate

    while (backfilledDate.isBefore(backfillEndDate.plusDays(1))) {
      CustomersDailyStatsGenerationJob.main(Array(backfilledDate.format(DateTimeFormatter.ISO_LOCAL_DATE)))
      backfilledDate = backfilledDate.plusDays(1)
    }
  }

}