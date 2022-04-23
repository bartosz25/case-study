package bartosz.ingestion.customers

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

case class Customer(customer_id: String, customer_unique_id: String,
                    customer_zip_code_prefix: String, customer_city: String,
                    customer_state: String)
object Customer {

  val Schema = ScalaReflection.schemaFor[Customer].dataType.asInstanceOf[StructType]

  val CustomerId = "customer_id"
  assert(Schema.fieldNames.contains(CustomerId))
  val CustomerUniqueId = "customer_unique_id"
  assert(Schema.fieldNames.contains(CustomerUniqueId))

}