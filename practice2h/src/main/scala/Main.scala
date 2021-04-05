object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
  val df1 = spark.range(0, 5)
  val df2 = spark.range(2, 7)
  //Ex1.1
//  df1.join(df2, df1.col("id") === df2.col("id")).show()
//  df1.join(df2, df1.col("id") === df2.col("id"), "inner").show()
  //Ex1.2
//  df1.join(df2, df1.col("id") === df2.col("id"), "left").show()
  //Ex1.3
//  df1.join(df2, df1.col("id") === df2.col("id"), "right").show()
  //Ex1.4
//  df1.join(df2, df1.col("id") === df2.col("id"), "left_anti").show()
  //Ex1.5
  df1.join(df2, df1.col("id") === df2.col("id"), "left").select(df1.col("id") as "id_1", df2.col("id") as "id_2").show()
}
