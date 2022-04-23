package bartosz.ingestion.orders

import bartosz.ingestion.CustomerBucketsNumber
import bartosz.model.PartitionData
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrdersDataIngestionJob {

  def main(args: Array[String]): Unit = {
    val ordersPath = args(0)
    val orderItemsPath = args(1)

    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - orders")
      .master("local[*]")
      // Dataset used only by Spark 3.0+ clients
      .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .enableHiveSupport()
      .getOrCreate()

    val ordersRaw = sparkSession.read.schema(Order.Schema).option("header", "true").csv(ordersPath)
    val orderItemsRaw = sparkSession.read.schema(OrderItem.Schema).option("header", "true").csv(orderItemsPath)

    val itemsByOrderId = Transformations.flattenAndEnrichItemsWithTotalPrices(orderItemsRaw)
    val denormalizedOrders = Transformations.combineOrdersWithItemsAndDecorateWithPartitionInfo(ordersRaw,
      itemsByOrderId)

    denormalizedOrders.write
      .mode(SaveMode.Overwrite)
      .partitionBy(PartitionData.YearColumn, PartitionData.MonthColumn, PartitionData.DayColumn,
        PartitionData.HourColumn)
      .bucketBy(CustomerBucketsNumber, Order.CustomerId)
      .saveAsTable("orders")

  }
}

object OrdersDataIngestionJobLocalIngestion {

  def main(args: Array[String]): Unit = {
    OrdersDataIngestionJob.main(Array(
      "/tmp/orders.csv",
      "/tmp/items.csv"
    ))
  }
}