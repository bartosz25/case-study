package bartosz.statistics.repeaters

import bartosz.ingestion.customers.Customer
import org.apache.spark.sql.{DataFrame, functions}

object Transformations {

  def countRepeaters(customersDailyStats: DataFrame, customers: DataFrame): Long = {
    val repeatersCount = customersDailyStats.join(customers, Seq(Customer.CustomerId))
      .select(Customer.CustomerUniqueId)
      .groupBy(Customer.CustomerUniqueId)
      .agg(
        functions.count(Customer.CustomerUniqueId).as("order_days_count")
      )
      .where("order_days_count >= 2")
      .count()

    repeatersCount
  }

}
