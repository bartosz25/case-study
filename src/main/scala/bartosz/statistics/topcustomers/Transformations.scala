package bartosz.statistics.topcustomers

import bartosz.statistics.dailystats.DailyStats
import org.apache.spark.sql.{DataFrame, functions}

object Transformations {

  def getTopCustomers(customersDailyStats: DataFrame): DataFrame = {
    customersDailyStats.select(DailyStats.CustomerId, DailyStats.TotalAmount)
      .groupBy(DailyStats.CustomerId)
      .agg(
        functions.sum(DailyStats.TotalAmount).as("total_spendings")
      )
      .orderBy(functions.col("total_spendings").desc)
  }

}
