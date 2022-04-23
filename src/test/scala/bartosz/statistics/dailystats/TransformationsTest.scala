package bartosz.statistics.dailystats

import bartosz.SparkSessionTestContext
import bartosz.model.PartitionData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformationsTest extends AnyFlatSpec with Matchers with SparkSessionTestContext {
  import sparkSession.implicits._

  it should "correctly generate totals and filter out not requested orders" in {
    val ordersWithItems = Seq(
      OrderWithItemsToTest(2021, 12, 30, 10.00, 2.99, Seq(OrderItemToTest("a"), OrderItemToTest("b"))),
      OrderWithItemsToTest(2021, 12, 30, 14.44, 0.15, Seq(OrderItemToTest("c"), OrderItemToTest("d"))),
      OrderWithItemsToTest(2021, 12, 29, 10.00, 2.10, Seq(OrderItemToTest("e"), OrderItemToTest("f"))),
      OrderWithItemsToTest(2021, 12, 30, 100.30, 1.10, Seq(OrderItemToTest("g"), OrderItemToTest("h"), OrderItemToTest("i"))),
    )
    val ordersWithItemsDataFrame = ordersWithItems.toDF

    val ordersForAggregation = Transformations.prepareOrdersForAggregation(ordersWithItemsDataFrame,
      PartitionData(2021, 12, 30))

    val ordersToAssert = ordersForAggregation.as[OrderWithItemsToTest].collect()

    ordersToAssert should have size 3
    ordersToAssert should contain allOf(ordersWithItems(0), ordersWithItems(1), ordersWithItems(3))
  }

  it should "find correct aggregates for the customers" in {
    val ordersToAggregate = Seq(
      OrderToAggregate("customer1", 100.00, 3),
      OrderToAggregate("customer1", 10.00, 5),
      OrderToAggregate("customer1", 444.44, 1),
      OrderToAggregate("customer1", 1.00, 1),
      OrderToAggregate("customer2", 1.00, 1)
    ).toDF
    val aggregatedCustomersOrders = Transformations.aggregatePreparedOrders(ordersToAggregate)
    val schema = aggregatedCustomersOrders.schema

    val resultToAssert = aggregatedCustomersOrders.map(row => {
      schema.fieldNames.map(fieldName => f"${fieldName}=${row.getAs[Object](fieldName).toString}").mkString(", ")
    }).collect()

    resultToAssert should have size 2
    resultToAssert should contain allOf(
      "customer_id=customer1, number_of_orders=4, total_amount=555.44, all_items=10, cheapest_order=1.0, most_expensive_order=444.44",
      "customer_id=customer2, number_of_orders=1, total_amount=1.0, all_items=1, cheapest_order=1.0, most_expensive_order=1.0"
    )
  }

}

case class OrderWithItemsToTest(
                                 year: Int, month: Int, day: Int,
                                 total_price: Double, total_freight_value: Double,
                                 items: Seq[OrderItemToTest]
                               )
case class OrderItemToTest(name: String)
case class OrderToAggregate(customer_id: String, amount: Double, items_in_order: Int)