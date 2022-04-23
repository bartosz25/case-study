package bartosz.ingestion.products

import org.apache.spark.sql.{SaveMode, SparkSession}

object ProductsDataIngestionJob {

  def main(args: Array[String]) = {
    val productsPath = args(0)

    val sparkSession = SparkSession.builder()
      .appName("Data ingestion job - products")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val productsRaw = sparkSession.read.schema(Product.Schema).option("header", "true").csv(productsPath)

    productsRaw.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("products")
  }
}


object ProductsDataIngestionJobLocalIngestion {

  def main(args: Array[String]) = {
    ProductsDataIngestionJob.main(Array("/tmp/products.csv"))
  }
}