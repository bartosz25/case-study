package bartosz.statistics.topcustomers

import bartosz.SparkSessionTestContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformationsTest extends AnyFlatSpec with Matchers with SparkSessionTestContext {

  import sparkSession.implicits._

  it should "get the most generous customers" in {
    val customersWithTotalOrderAmounts = Seq(
      ("c1", 100.0), ("c2", 235.0),
      ("c1", 55.99), ("c3", 1.0),
      ("c1", 49.11)
    ).toDF("customer_id", "total_amount")

    val topCustomers = Transformations.getTopCustomers(customersWithTotalOrderAmounts)

    val topCustomersToAssert = topCustomers
      .map(row => (row.getAs[String]("customer_id"), row.getAs[Double]("total_spendings")))
      .collect()

    topCustomersToAssert should have size 3
    topCustomersToAssert should contain theSameElementsInOrderAs Seq(
      ("c2", 235.0), ("c1", 205.10000000000002), ("c3", 1.0)
    )
  }

}
