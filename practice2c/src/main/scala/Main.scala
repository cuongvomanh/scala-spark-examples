import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main extends App{
  val spark = config.spark.spark()
//  val sparkConf = new SparkConf().set("spark.hadoop.validateOutputSpecs", "false")
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
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

  val df = spark.read.schema(schema).parquet(readPath)
//  df.show()

  val writePath = hdfsPath + "/dgd2/sparksql/out/2c/csv/dim"

  df.write.option("header", true).option("sep","\t").csv(writePath)
  spark.read.option("header", true).option("inferschema", true).option("sep", "\t").csv(writePath).show()



  df.write.option("header", false).option("sep",",").mode("overwrite").csv(writePath)
  spark.read.option("header", false).option("sep", ",").csv(writePath).show()

}
