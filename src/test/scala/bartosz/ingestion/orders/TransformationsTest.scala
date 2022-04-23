package bartosz.ingestion.orders

import bartosz.SparkSessionTestContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class TransformationsTest extends AnyFlatSpec with Matchers with SparkSessionTestContext {

  import sparkSession.implicits._
  private val orderItems = Seq(
    orderItemWithDefaults("order1", 30.99, 10.00),
    orderItemWithDefaults("order1", 11.12, 11.11),
    orderItemWithDefaults("order2", 30.99, 14.44),
    orderItemWithDefaults("order3", 1.99, 15.55),
    orderItemWithDefaults("order1", 3.00, 0),
  )

  it should "group items by id and enrich them with totals" in {
    val flattenedOrderItems = Transformations.flattenAndEnrichItemsWithTotalPrices(orderItems.toDF)

    val flattenedOrderToAssert = flattenedOrderItems.as[FlattenedOrderToAssert].collect()

    flattenedOrderToAssert should have size 3
    flattenedOrderToAssert should contain allOf(
      FlattenedOrderToAssert("order1", orderItems.filter(_.order_id == "order1").map(toOrderItemNoOrderId(_)),
        45.11, 21.11),
      FlattenedOrderToAssert("order2", orderItems.filter(_.order_id == "order2").map(toOrderItemNoOrderId(_)),
        30.99, 14.44),
      FlattenedOrderToAssert("order3", orderItems.filter(_.order_id == "order3").map(toOrderItemNoOrderId(_)),
        1.99, 15.55)
    )
  }

  it should "combine orders with items and generate partition info" in {
    val orders = Seq(
      ("order1", Timestamp.valueOf("2022-03-03 20:03:00")),
      ("order2", Timestamp.valueOf("2022-05-15 08:15:00")),
      ("order3", Timestamp.valueOf("2022-03-30 15:50:00")),
    )

    val combinedOrdersWithPartitionInfo = Transformations.combineOrdersWithItemsAndDecorateWithPartitionInfo(
      orders.toDF("order_id", "order_purchase_timestamp"),
      orderItems.toDF
    )
    val combinedOrdersToAssert = combinedOrdersWithPartitionInfo.as[CombinedOrdersToAssert].collect()

    combinedOrdersToAssert should have size 5
    combinedOrdersToAssert should contain allOf(
      CombinedOrdersToAssert("order1", orders(0)._2, 3.0, 0.0, year = 2022, month = 3, day = 3, hour = 20),
      CombinedOrdersToAssert("order1", orders(0)._2, 11.12, 11.11, year = 2022, month = 3, day = 3, hour = 20),
      CombinedOrdersToAssert("order1", orders(0)._2, 30.99, 10.0, year = 2022, month = 3, day = 3, hour = 20),
      CombinedOrdersToAssert("order2", orders(1)._2, 30.99, 14.44, year = 2022, month = 5, day = 15, hour = 8),
      CombinedOrdersToAssert("order3", orders(2)._2, 1.99, 15.55, year = 2022, month = 3, day = 30, hour = 15),
    )
  }

  private def orderItemWithDefaults(orderId: String, price: Double, freightValue: Double) = {
    OrderItem(
      order_id = orderId, price = price, freight_value = freightValue,
      order_item_id = 0, product_id = "abc", seller_id = "123",
      shipping_limit_date = new Timestamp(1)
    )
  }

  private def toOrderItemNoOrderId(orderItem: OrderItem): OrderItemNoOrderId = {
    OrderItemNoOrderId(
      order_item_id = orderItem.order_item_id, product_id = orderItem.product_id, seller_id = orderItem.seller_id,
      shipping_limit_date = orderItem.shipping_limit_date, price = orderItem.price, freight_value = orderItem.freight_value
    )
  }

}

case class FlattenedOrderToAssert(
                                 order_id: String,
                                 items: Seq[OrderItemNoOrderId],
                                 total_price: Double, total_freight_value: Double
                                 )
case class OrderItemNoOrderId(order_item_id: Int, product_id: String, seller_id: String,
                              shipping_limit_date: Timestamp, price: Double, freight_value: Double)

case class CombinedOrdersToAssert(order_id: String, order_purchase_timestamp: Timestamp,
                                  price: Double, freight_value: Double,
                                  year: Int, month: Int, day: Int, hour: Int)