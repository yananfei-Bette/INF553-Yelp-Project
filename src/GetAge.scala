import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{Map, Set}

object GetAge {
  def main(args: Array[String]): Unit = {

    var businessYear : Map[String, Set[Int]] = Map()
    var businessAge : Map[String, Int] = Map()
    val spark: SparkSession = SparkSession
      .builder()
      .appName("task1")
      .master("local[*]")
      .getOrCreate()

    val input1: DataFrame = spark.read
      .option("inferSchema", "true")
      .json("yelp_academic_dataset_review.json")
    
    val a = input1.select("business_id", "date")
    val b = a.collect().map(line => (line.toString().split(",")(0),line.toString().split(",")(1)))
    val c = b.foreach(line => {
      if (businessYear.contains(line._1)) {
        businessYear(line._1) += line._2.split("-")(0).toInt
      } else {
        var temp : Set[Int] = Set()
        temp += line._2.split("-")(0).toInt
        businessYear += (line._1 -> temp)
      }
    })
    businessYear.foreach(unit => {
      var max = unit._2.max
      var min = unit._2.min
      var age = max - min + 1
      businessAge += (unit._1 -> age)
    })

    val p = new PrintWriter("business_age.csv")
    businessAge.foreach(line => {
      p.print(line._1.replace("[",""))
      p.print(",")
      p.print(line._2)
      p.println()
    })
    p.close()
  }
}
