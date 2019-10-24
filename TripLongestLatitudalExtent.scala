import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.min

object TripLongestLatitudalExtent {

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


      val selectedDurations = df.select($"start_station_id", $"end_station_id", $"start_station_latitude", $"end_station_latitude")

      val subtrakted = selectedDurations.withColumn("Lat_difference", expr("start_station_latitude - end_station_latitude"))

      val maximum = subtrakted.agg(max("Lat_difference"))
      val minimum = subtrakted.agg(min("Lat_difference"))

      val joined = maximum.crossJoin(minimum)

      df.printSchema()




  }


}
