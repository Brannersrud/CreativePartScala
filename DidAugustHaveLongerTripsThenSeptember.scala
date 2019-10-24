import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.expr

object DidAugustHaveLongerTripsThenSeptember {

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

    val sparkAug = SparkSession.builder().getOrCreate()
    val dfWay = sparkAug.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/bikeextractsaugust2019.csv")
    import spark.implicits._


    val selectedSeptDuration = df.select($"duration")
    val avgDurationSept = selectedSeptDuration.agg(avg($"duration").as("average_trip_duration_in_September"))


    val selectedAugDuration = dfWay.select($"duration")
    val avgDurationAug = selectedAugDuration.agg(avg($"duration").as("average_trip_duration_in_August"))
    val joined = avgDurationAug.crossJoin(avgDurationSept)

    val difference = joined.withColumn("Difference in duration", expr("average_trip_duration_in_August - average_trip_duration_in_September"))

    difference.show()

  }

}
