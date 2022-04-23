package bartosz.statistics.repeaters

import bartosz.SparkSessionTestContext
import bartosz.ingestion.customers.Customer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformationsTest extends AnyFlatSpec with Matchers with SparkSessionTestContext {

  it should "correctly detect repeaters" in {
    import sparkSession.implicits._
    val customers = Seq(
      customerWithDefaults("111", "1"), customerWithDefaults("11", "1"),
      customerWithDefaults("222", "2"),
      customerWithDefaults("333", "3")
    ).toDF
    val customersDailyStats = Seq(
      ("111"), ("11"), ("111"),
      ("222"), ("222"),
      ("333")
    ).toDF("customer_id")

    val repeaters = Transformations.countRepeaters(customersDailyStats, customers)

    repeaters shouldEqual 2
  }

  private def customerWithDefaults(customerId: String, customerUniqueId: String): Customer = {
    Customer(
      customer_id = customerId, customer_unique_id = customerUniqueId,
      customer_zip_code_prefix = "75002", customer_city = "Paris", customer_state = "Ile de France"
    )
  }

}
