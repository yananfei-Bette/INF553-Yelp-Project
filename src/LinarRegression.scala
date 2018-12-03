import java.io.PrintWriter
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.Map

object LinarRegression {
  def main(args: Array[String]): Unit = {
    val p = new PrintWriter("business_star_lr_coef.csv")
    var businessReview : Map[String, List[String]] = Map()

    val spark: SparkSession = SparkSession
      .builder()
      .appName("task1")
      .master("local[*]")
      .getOrCreate()

    val input1: DataFrame = spark.read
      .option("inferSchema", "true")
      .json("yelp_academic_dataset_review.json")

    val input2: DataFrame = spark.read
      .option("inferSchema", "false")
      .json("vegas_list_business.csv")
    val d = input2.collect().flatMap(line => line.toString().replace("[", "").split(",")).map(word => word)
    val vegas = d.toSet


    val a = input1.select("business_id", "date", "stars")
    val b = a.collect().map(line => (line.toString().split(",")(0).replace("[",""),line.toString().split(",")(1), line.toString().split(",")(2).replace("]","")))
    val c = b.foreach(line => {
      if (businessReview.contains(line._1)) {
        businessReview(line._1) = businessReview(line._1) :+ (line._2.replace("-", "") + "," + line._3)
      } else {
        var temp : List[String] = List()
        temp = temp :+ (line._2.replace("-", "") + "," + line._3)
        businessReview += (line._1 -> temp)
      }
    })
    businessReview.foreach(line => {
      if (vegas.contains(line._1)) {
        import spark.implicits._
        val data1 = line._2.map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).sortBy(_._1)
        val data = data1.toDF("date", "star")
        val colArray1 = Array("date")

        val assembler = new VectorAssembler().setInputCols(colArray1).setOutputCol("features")
        val vecDF1: DataFrame = assembler.transform(data)
        val lr1 = new LinearRegression()
        val lr2 = lr1.setFeaturesCol("features").setLabelCol("star").setFitIntercept(true)
        val lr3 = lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
        val lr_new = lr3

        val lrModel = lr_new.fit(vecDF1)
        lrModel.extractParamMap()
        // Print the coefficients and intercept for linear regression
        p.print(line._1)
        p.print(",")
        p.print(lrModel.coefficients)
        p.println()
      }
    })
    p.close()
  }
}

