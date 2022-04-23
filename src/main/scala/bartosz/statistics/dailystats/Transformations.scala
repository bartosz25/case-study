package bartosz.statistics.dailystats

import bartosz.ingestion.orders.{FlattenedOrderItems, Order}
import bartosz.model.PartitionData
import org.apache.spark.sql.{DataFrame, functions}

object Transformations {

  def prepareOrdersForAggregation(orders: DataFrame, partitionDataToRead: PartitionData): DataFrame = {
    orders.where(
      f"""
        |year = ${partitionDataToRead.year} AND month = ${partitionDataToRead.month}
        |AND day = ${partitionDataToRead.day}
        |""".stripMargin)
      .withColumn("amount",
        orders(FlattenedOrderItems.TotalPrice) + orders(FlattenedOrderItems.TotalFreightValue))
      .withColumn("items_in_order",
        functions.size(orders(FlattenedOrderItems.Items)))
  }

  def aggregatePreparedOrders(ordersToAggregate: DataFrame): DataFrame = {
    ordersToAggregate.groupBy(Order.CustomerId)
      .agg(
        functions.count("*").as("number_of_orders"),
        functions.sum("amount").as(DailyStats.TotalAmount),
        functions.sum("items_in_order").as("all_items"),
        functions.min("amount").as("cheapest_order"),
        functions.max("amount").as("most_expensive_order")
      )
  }

}
