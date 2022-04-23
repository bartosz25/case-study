package bartosz.model

case class PartitionData(year: Int, month: Int, day: Int, hour: Option[Int] = None)

object PartitionData {

  val YearColumn = "year"
  val MonthColumn = "month"
  val DayColumn = "day"
  val HourColumn = "hour"

  def fromDateString(date: String): PartitionData = {
    val dateToRead = date.split("-").map(_.toInt)
    val Array(year, month, day) = dateToRead
    PartitionData(year, month, day)
  }

}