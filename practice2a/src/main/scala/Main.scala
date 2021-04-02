import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.util.Random

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  // Ex1
  // Ex1.1
//  val rdd = sc.parallelize(1 to 10).map(x => (x, Random.nextInt(x)))
//  val intDf = rdd.toDF("key", "value")
//  intDf.show()
//  intDf.printSchema()
  // Ex1.2
  val rowRdd = sc.parallelize(Array(Row(1L, "cuongvm12", 30L), Row(2L, "congnt", 40L)))
  val schema = StructType(Array(StructField("Id", LongType, true), StructField("Name", StringType, true), StructField("Salary", LongType, false)))
  val employeeDf = spark.createDataFrame(rowRdd, schema)
  employeeDf.printSchema()
  // Ex1.3
}
