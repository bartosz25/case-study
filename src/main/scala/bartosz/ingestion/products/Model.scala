package bartosz.ingestion.products

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


case class Product(product_id: String, product_category_name: String,
                   product_name_lenght: Double, product_description_lenght: Double,
                   product_photos_qty: Double, product_weight_g: Double,
                   product_length_cm: Double, product_height_cm: Double,
                   product_width_cm: Double, product_category_name_english: String
                  )
object Product {
  val Schema = ScalaReflection.schemaFor[Product].dataType.asInstanceOf[StructType]
}