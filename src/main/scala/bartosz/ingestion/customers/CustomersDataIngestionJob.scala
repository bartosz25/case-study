package bartosz.ingestion.customers

import bartosz.ingestion.CustomerBucketsNumber
import org.apache.spark.sql.{SaveMode, SparkSession}

object CustomersDataIngestionJob {

  def main(args: Array[String]) = {
    val inputPath = args(0)

    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - customers")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val customersRaw = sparkSession.read
      .schema(Customer.Schema).option("header", "true").csv(inputPath)

    customersRaw.write
      .mode(SaveMode.Overwrite)
      .bucketBy(CustomerBucketsNumber, Customer.CustomerId)
      .format("parquet")
      .saveAsTable("customers")
  }
}

object CustomersDataIngestionJobLocalIngestion {

  def main(args: Array[String]): Unit = {
    CustomersDataIngestionJob.main(Array("/tmp/customer.csv"))
  }
}