import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
  // Ex1
  val countryPath = hdfsPath + "/dgd2/sparksql/in/parquet/dim"
  val rawCountry = spark.read.parquet(countryPath)
  val country = rawCountry.filter('iso_code.isNotNull && 'iso_code =!= "OWID_WRL")
//  country.show()
  //Ex1.1
  println(country.count())
  //426
  //Ex1.2
//  println(country.select('iso_code).distinct().count())
  //213
  //Ex1.3
//  country.select('continent).distinct().show()
//  country.where('continent === "Europe").select(min('gdp_per_capita)).show()
//  country.where('continent === "Africa").select(max('gdp_per_capita)).show()
  //Ex1.4
//  country.select(sum('population)).show()
  //Ex1.5
//  country.select(avg('gdp_per_capita)).show()

}
