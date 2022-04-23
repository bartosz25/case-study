package bartosz.ingestion.orders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DecimalType, DoubleType, StructType}

import java.sql.Timestamp

case class Order(order_id: String, customer_id: String, order_status: String,
                 order_purchase_timestamp: Timestamp, order_approved_at: Timestamp,
                 order_delivered_carrier_date: Timestamp, order_delivered_customer_date: Timestamp,
                 order_estimated_delivery_date: Timestamp)
object Order {
  val Schema = ScalaReflection.schemaFor[Order].dataType.asInstanceOf[StructType]

  val CustomerId = "customer_id"
  assert(Schema.fieldNames.contains(CustomerId))
  val OrderId = "order_id"
  assert(Schema.fieldNames.contains(OrderId))
  val OrderPurchaseTimestamp = "order_purchase_timestamp"
  assert(Schema.fieldNames.contains(OrderPurchaseTimestamp))
}


case class OrderItem(order_id: String, order_item_id: Int, product_id: String, seller_id: String,
                     shipping_limit_date: Timestamp, price: Double, freight_value: Double)
object OrderItem {
  private val fields = ScalaReflection.schemaFor[OrderItem].dataType.asInstanceOf[StructType].fields
    .map(field => {
      // Change the precision because Spark maps the `Double` into a `DoubleType` (
      val fieldType = field.dataType match {
        case _: DoubleType => new DecimalType(10, 2)
        case other => other
      }
      field.copy(dataType = fieldType)
    })
  val Schema = StructType(fields)
}
object FlattenedOrderItems {
  val Items = "items"
  val TotalPrice = "total_price"
  val TotalFreightValue = "total_freight_value"
}