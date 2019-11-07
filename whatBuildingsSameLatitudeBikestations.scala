import org.apache.spark.sql.functions.{explode, round}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object whatBuildingsSameLatitudeBikestations {

  def main(args: Array[String]): Unit = {
    val d = new SparkConf()
      .setMaster("local")
      .setAppName("Just a test")
    val sc = new SparkContext(d)
    sc.setLogLevel("ERROR")
    val sparkmain = SparkSession.builder().getOrCreate()

    val latBikes = getLatitudeBikes(sparkmain)
    val nodeDf= getLatitudeBuildings(sparkmain)

    val joined = latBikes.crossJoin(nodeDf)
    removeBuildingsThatAreFarAway(joined, sparkmain)
  }

  def removeBuildingsThatAreFarAway(df: DataFrame, sparkSession: SparkSession) {
    import sparkSession.implicits._
    val latDiff = df.filter(($"start_bike_lat".cast(DoubleType) % $"_lat".cast(DoubleType) < 0.005) && $"end_bike_lon".cast(DoubleType) % $"_lon".cast(DoubleType) < 0.005)
    latDiff.show(10000)
  }

  def getLatitudeBikes(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("input/bikeextractseptember2019.csv")

    val lat = df.select( $"start_station_id").groupBy( $"start_station_id").count()
    val lon = df.select($"start_station_id", $"start_station_name",round($"start_station_longitude", 6).as("end_bike_lon"), round($"start_station_latitude", 6).as("start_bike_lat"))
    val joined = lat.join(lon, lat("start_station_id") === lon("start_station_id"))

   joined
  }


  def getLatitudeBuildings(spark : SparkSession): DataFrame = {
    import spark.implicits._
    val dfOsm = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "osm")
      .option("rowTag", "node")
      .load("input/osloosm.osm")

    val latBuild = dfOsm.select($"_id", $"_lon", $"_lat", explode($"tag").as("buildingTags"))
    val withcolumn = latBuild.withColumn("Latitude_building", round($"_lat".cast(DoubleType), 6))
    val lonbuild = withcolumn.withColumn("Long_building", round($"_lon".cast(DoubleType), 6))
    val filteredBuilding = lonbuild.filter($"buildingTags._k" === "building")


    filteredBuilding
  }


}
