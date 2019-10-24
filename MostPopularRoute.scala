import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}



object AnalyzeData {

  def main(args: Array[String]){
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

    import spark.implicits._

    //which route was the most popular in september
    val filterIdAndStation = df.select($"start_station_id", $"start_station_name",$"end_station_id", $"end_station_name")

    val grouped = filterIdAndStation.groupBy($"start_station_name", $"end_station_name").count()

    val sorted = grouped.orderBy($"count".desc)

    sorted.show(10)


    df.printSchema()




  }

}
