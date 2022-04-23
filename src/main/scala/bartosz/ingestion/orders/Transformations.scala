package bartosz.ingestion.orders

import bartosz.ingestion.orders.Order.OrderPurchaseTimestamp
import bartosz.model.{PartitionData}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{DataFrame, functions}

object Transformations {

  private val ItemFieldsToFlattenAsColumns = OrderItem.Schema.fieldNames.filter(name => name != Order.OrderId)
    .map(fieldName => functions.col(fieldName))

  def flattenAndEnrichItemsWithTotalPrices(orderItems: DataFrame): DataFrame = {
    orderItems.select(functions.col(Order.OrderId), functions.struct(ItemFieldsToFlattenAsColumns: _*).as("item"))
      .groupBy(Order.OrderId)
      .agg(
        collect_list("item").as(FlattenedOrderItems.Items),
        functions.sum("item.price").as(FlattenedOrderItems.TotalPrice),
        functions.sum("item.freight_value").as(FlattenedOrderItems.TotalFreightValue)
      )
  }

  def combineOrdersWithItemsAndDecorateWithPartitionInfo(orders: DataFrame, enrichedOrderItems: DataFrame): DataFrame = {
    orders.join(enrichedOrderItems, Seq(Order.OrderId), "leftouter")
      .withColumn(PartitionData.YearColumn, functions.year(functions.col(OrderPurchaseTimestamp)))
      .withColumn(PartitionData.MonthColumn, functions.month(functions.col(OrderPurchaseTimestamp)))
      .withColumn(PartitionData.DayColumn, functions.dayofmonth(functions.col(OrderPurchaseTimestamp)))
      .withColumn(PartitionData.HourColumn, functions.hour(functions.col(OrderPurchaseTimestamp)))
  }

}
