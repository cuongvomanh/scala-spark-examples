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
//  country.groupBy('continent).count().show()
//  country.groupBy('continent).agg(count('iso_code)).show()
  //Ex2\
//  country.groupBy('continent).agg(sum('population)).show()
  //Ex3
//  country.filter('median_age <65).groupBy('continent).agg(count('iso_code)).show()
  //Ex4
//  country.groupBy('continent).agg(min('gdp_per_capita)).show()
  //Pr1
//  val charCountry = country.withColumn("char",'location.substr(0,1)).groupBy('char).agg(count('iso_code) as "num_contries").sort('char)
//  val charCountryPath = hdfsPath + "/dgd2/sparksql/out/2f/csv/dim"
//  charCountry.write.option("header", true).mode("overwrite").parquet(charCountryPath)
//  spark.read.option("header", true).parquet(charCountryPath).show()
  //Pr2
  val continentDf = country.groupBy('continent).agg(count('iso_code) as "num_countries", sum('population) as "continent_pop", sum('hospital_beds_per_thousand*'population/1000) as "num_beds")
  val continentPath = hdfsPath + "/dgd2/sparksql/out/2f/csv/dim"
  continentDf.write.option("header", true).mode("overwrite").parquet(continentPath)
  spark.read.option("header", true).parquet(continentPath).show()

}
