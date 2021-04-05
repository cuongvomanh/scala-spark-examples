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
  //Ex1
//  val ds = spark.range(1,5)
//  ds.filter(a=> a>3).show()
//  ds.toDF("id").filter('id > 3).show()
  //Ex2
  case class Country (iso_code: Option[String], continent: Option[String], location: Option[String], population: Option[Double], median_age: Option[Double], gdp_per_capita: Option[Double], hospital_beds_per_thousand: Option[Double], life_expectancy: Option[Double])
  val ds = rawCountry.as[Country]
  //Ex2.1
//  ds.printSchema()
  //Ex2.2
//  ds.filter('population > 10000000).show()
//  ds.filter(a => a.population.map(b => b > 10000000).getOrElse(false)).show()
  //Ex2.3
//  ds.groupBy('continent).count().show()
  ds.filter(a => a.gdp_per_capita.map(g => g > 2000).getOrElse(false) && a.continent.map(l => l == "Africa").getOrElse(false)).show()
}
