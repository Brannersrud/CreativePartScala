import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.expr

object AverageTripDuration {

  def main(args: Array[String]) {
    val d = new SparkConf()
      .setMaster("local")
      .setAppName("Just a test")
    val sc = new SparkContext(d)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/bikeextractseptember2019.csv")

    //average duration of a biketrip
    import spark.implicits._


    val selectedDurations = df.select($"duration")
    val avgDuration = selectedDurations.agg(avg($"duration").as("average_trip_duration_in_seconds"))
    val inMinutes = avgDuration.withColumn("average_trip_duration_in_minutes", expr("average_trip_duration_in_seconds / 60"))
    inMinutes.show()

  }

}
