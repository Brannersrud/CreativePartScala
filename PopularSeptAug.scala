import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PopularSeptAug {

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
    val dfAug = sparkAug.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/bikeextractsaugust2019.csv")

    import spark.implicits._

    val selectedAugRoutes = dfAug.select($"start_station_id", $"start_station_name",$"end_station_id", $"end_station_name")

    val groupedAug = selectedAugRoutes.groupBy($"start_station_name", $"end_station_name").count().orderBy($"count".asc)


    val selectedSeptRoutes = df.select($"start_station_id", $"start_station_name",$"end_station_id", $"end_station_name")

    val groupedSept = selectedSeptRoutes.groupBy($"start_station_name", $"end_station_name").count().orderBy($"count".asc)

    val joined = groupedAug.crossJoin(groupedSept)

    joined.show(1)






  }

}
