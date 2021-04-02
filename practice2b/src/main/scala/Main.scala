import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  val sqlContext = spark.sqlContext
  import sqlContext.implicits
  // Push file in README.md
  // Ex1
  val covidCsvPath = hdfsPath + "/dgd2/sparksql/in/csv/dim1/"
  val covidCsvHeaderPath = hdfsPath + "/dgd2/sparksql/in/csv/dim2/"
  val covidJsonPath = hdfsPath + "/dgd2/sparksql/in/json/dim/"
  val covidParquetPath = hdfsPath + "/dgd2/sparksql/in/parquet/dim/"
  val covidOrcPath = hdfsPath + "/dgd2/sparksql/in/orc/dim/"
  val schema=StructType(Array(StructField("iso_code", StringType, true),
    StructField("continent", StringType, true),
    StructField("location", StringType, true),
    StructField("population", DoubleType, true),
    StructField("median_age", DoubleType, true),
    StructField("gdp_per_capita", DoubleType, true),
    StructField("hospital_beds_per_thousand", DoubleType, true),
    StructField("life_expectancy", DoubleType, true)
  ))
  // Ex1.1
//  val covidCsv = spark.read.csv(covidCsvPath)
//  covidCsv.show()
  // Ex1.2

//  val covidCsv = spark.read.schema(schema).csv(covidCsvPath)
//  covidCsv.show()
//  covidCsv.printSchema()
  // Ex1.3
//  val covidCsv = spark.read.schema(schema).option("header", "true").option("infrerSchema", "true").csv(covidCsvHeaderPath)
//  covidCsv.printSchema()
//  covidCsv.show()
  // Ex1.4
//  val covidJson = spark.read.json(covidJsonPath)
//  covidJson.show()
  // Ex1.5
//  val covidParquet = spark.read.schema(schema).parquet(covidParquetPath)
//  covidParquet.show()
  // Ex1.6
  val covidOrc = spark.read.schema(schema).orc(covidOrcPath)
  covidOrc.show()

}
