import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
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
  //Ex1
//  covid.groupBy('continent).count().show()
//  covid.groupBy('continent).agg(count('iso_code)).show()
  //Ex2
//  covid.groupBy('continent).agg(sum('population)).show()
  //Ex3
//  covid.filter('median_age <65).groupBy('continent).agg(count('iso_code)).show()
  //Ex4
//  covid.groupBy('continent).agg(min('gdp_per_capita)).show()
  //Pr1
//  val charCountry = covid.withColumn("char",'location.substr(0,1)).groupBy('char).agg(count('iso_code) as "num_contries").sort('char)
//  val charCountryPath = hdfsPath + "/dgd2/sparksql/out/2f/csv/dim"
//  charCountry.write.option("header", true).mode("overwrite").parquet(charCountryPath)
//  spark.read.option("header", true).parquet(charCountryPath).show()
  //Pr2
  val continentDf = covid.groupBy('continent).agg(count('iso_code) as "num_countries", sum('population) as "continent_pop", sum('hospital_beds_per_thousand*'population/1000) as "num_beds")
  val continentPath = hdfsPath + "/dgd2/sparksql/out/2f/csv/dim"
  continentDf.write.option("header", true).mode("overwrite").parquet(continentPath)
  spark.read.option("header", true).parquet(continentPath).show()

}
