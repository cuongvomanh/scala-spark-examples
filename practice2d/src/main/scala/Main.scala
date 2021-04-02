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
  val readPath = hdfsPath + "/dgd2/sparksql/in/parquet/dim"
  val schema=StructType(Array(StructField("iso_code", StringType, true),
    StructField("continent", StringType, true),
    StructField("location", StringType, true),
    StructField("population", DoubleType, true),
    StructField("median_age", DoubleType, true),
    StructField("gdp_per_capita", DoubleType, true),
    StructField("hospital_beds_per_thousand", DoubleType, true),
    StructField("life_expectancy", DoubleType, true)
  ))
  val raw = spark.read.schema(schema).parquet(readPath)
  val covid = raw.filter('iso_code.isNotNull && 'iso_code =!= "OWID_WRL")
//  covid.show()
  //Ex1.1
//  covid.select("location").show()
  //Ex1.2
//  covid.select('iso_code, 'location).show()
  //Ex1.3
//  covid.filter('continent === "Asia").where('gdp_per_capita > 2000).show()
//  covid.filter('iso_code.isNull).show()
  //Ex1.4
//  covid.select('continent).distinct().show()
  //Ex1.5
//  val exDf = covid.select('iso_code, 'continent, 'population, 'gdp_per_capita)
//  exDf.sort('population).show()
//  exDf.sort('continent, 'gdp_per_capita.asc).show(1000)
//  covid.sort('iso_code).show()
  //Ex1.6
//  covid.where('gdp_per_capita.isNotNull).sort('gdp_per_capita.desc).limit(10).show()
  //Ex1.7
//  spark.range(1,5).union(spark.range(2,6)).show()
  //Ex1.8
//  covid.withColumn("is_valide", when('iso_code.isNotNull, 1).otherwise(0)).show()
  //Ex1.9
//  covid.withColumn("number_bed_of_nation", 'population/1000*'hospital_beds_per_thousand).show()
  //Ex1.10
//  covid.withColumnRenamed("hospital_beds_per_thousand", "hbpt").show()
  //Ex1.11
//  covid.drop('median_age).show()
  //Pr1
  val asiaTop10 = covid.filter('continent === "Asia").sort('population.desc).limit(10).select('iso_code, 'continent, 'location, 'population).withColumnRenamed("iso_code", "code")
  val asiaTop10Path = hdfsPath + "/dgd2/sparksql/out/2d/parquet/dim"
  asiaTop10.write.option("header", true).mode("overwrite").parquet(asiaTop10Path)
  spark.read.parquet(asiaTop10Path).show()




}
