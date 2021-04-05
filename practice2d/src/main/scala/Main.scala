import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
  // Push file in README.md
  // Ex1
  val countryPath = hdfsPath + "/dgd2/sparksql/in/parquet/dim"
  val rawCountry = spark.read.parquet(countryPath)
  val country = rawCountry.filter('iso_code.isNotNull && 'iso_code =!= "OWID_WRL")
//  country.show()
  //Ex1.1
//  country.select("location").show()
  //Ex1.2
//  country.select('iso_code, 'location).show()
  //Ex1.3
//  country.filter('continent === "Asia").where('gdp_per_capita > 2000).show()
//  country.filter('iso_code.isNull).show()
  //Ex1.4
//  country.select('continent).distinct().show()
  //Ex1.5
//  val exDf = country.select('iso_code, 'continent, 'population, 'gdp_per_capita)
//  exDf.sort('population).show()
//  exDf.sort('continent, 'gdp_per_capita.asc).show(1000)
//  country.sort('iso_code).show()
  //Ex1.6
//  country.where('gdp_per_capita.isNotNull).sort('gdp_per_capita.desc).limit(10).show()
  //Ex1.7
//  spark.range(1,5).union(spark.range(2,6)).show()
  //Ex1.8
//  country.withColumn("is_valide", when('iso_code.isNotNull, 1).otherwise(0)).show()
  //Ex1.9
//  country.withColumn("number_bed_of_nation", 'population/1000*'hospital_beds_per_thousand).show()
  //Ex1.10
//  country.withColumnRenamed("hospital_beds_per_thousand", "hbpt").show()
  //Ex1.11
//  country.drop('median_age).show()
  //Pr1
  val asiaTop10 = country.filter('continent === "Asia").sort('population.desc).limit(10).select('iso_code, 'continent, 'location, 'population).withColumnRenamed("iso_code", "code")
  val asiaTop10Path = hdfsPath + "/dgd2/sparksql/out/2d/parquet/dim"
  asiaTop10.write.option("header", true).mode("overwrite").parquet(asiaTop10Path)
  spark.read.parquet(asiaTop10Path).show()




}
