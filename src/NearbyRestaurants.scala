import org.apache.spark.Partitioner
import java.io.{File, PrintWriter}
import java.text.DecimalFormat
import java.math.RoundingMode
import org.apache.spark.sql.functions._

object NearbyRestaurants {

  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder.master("local[4]").appName("Task1").getOrCreate

    val df = spark.read.format("json").option("header", "true")
      .load(args(0)).filter("city == 'Las Vegas' OR city == 'Henderson' OR city == 'North Las Vegas'")
      .select("business_id", "latitude", "longitude")
      .filter("latitude is not null AND longitude is not null")
      .rdd

    val df_collected = df.collect()
    val nearby = df.map(row => {
      var b = scala.collection.mutable.ListBuffer[String]()
      df_collected.foreach(other => {
        val d = new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(row.get(1).toString.toDouble,
          row.get(2).toString.toDouble), Location(other.get(1).toString.toDouble, other.get(2).toString.toDouble))
        if (d <= 2) b += other.get(0).toString // 2 km
      })
      (row.get(0).toString, b.toList)
    }).collect()
    val pw = new PrintWriter(new File(args(1)))
    nearby.foreach(a => {
      pw.println("{\"business_id\":\"" + a._1 + "\", \"nearby\":[\"" + mkFoldLeftString(a._2, "\",\"") + "\"]},")
    })
    pw.close()

  }

  def mkFoldLeftString[A](list:List[String], delim:String = ","): String = list match { case head :: tail => tail.foldLeft(head)(_ + delim + _) case Nil => "" }

}


case class Location(lat: Double, lon: Double)
trait DistanceCalculator {
  def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
}
class DistanceCalculatorImpl extends DistanceCalculator {
  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
  override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userLocation.lat)) *
        Math.cos(Math.toRadians(warehouseLocation.lat)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }
}
