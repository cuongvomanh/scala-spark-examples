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
  //Ex1.1
  println(covid.count())
  //426
  //Ex1.2
//  println(covid.select('iso_code).distinct().count())
  //213
  //Ex1.3
//  covid.select('continent).distinct().show()
//  covid.where('continent === "Europe").select(min('gdp_per_capita)).show()
//  covid.where('continent === "Africa").select(max('gdp_per_capita)).show()
  //Ex1.4
//  covid.select(sum('population)).show()
  //Ex1.5
//  covid.select(avg('gdp_per_capita)).show()

}
