import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.explode

object scalaOsmOsl {


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

    //how many bikestations(here i focused on startstations)
    // exist in the extract
    import spark.implicits._
    val myExtract = df.select($"start_station_id")
    val mycount = myExtract.withColumn("count", $"start_station_id").distinct().groupBy("start_station_id").count()

    val mysum = mycount.agg(sum($"count").as("total bike stations"))


    val sparkOsm = SparkSession.builder().getOrCreate()
    val dfOsm = sparkOsm.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "way")
      .load("input/osloosm.osm")



    //how many types of way is a building in my extract
    val generalfilter = dfOsm.select($"_id", explode($"tag").as("nodetag"))

    val buildingfilter = generalfilter.filter($"nodetag._k" === "building").groupBy($"nodetag._k").count().as("Buildings in extract")

    val myJoinedQuery = buildingfilter.crossJoin(mysum)

    myJoinedQuery.show()

  }






}
