object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  import spark.implicits._
  import org.apache.spark.sql.functions._
  //  import spark.sqlContext.implicits._
  //Ex1
  //Ex1.1
//  val rawDateDf = Seq((1, "2020-02-20", "15/01-2020")).toDF("id", "date", "date_string")
//  rawDateDf.printSchema()
//  val dateDf = rawDateDf.select(to_date('date) as "date1", to_date('date_string, "dd/MM-yyyy") as "date2")
//  dateDf.show()
//  dateDf.select(datediff('date1, 'date2)).show()
//  rawDateDf.select(to_date('date) as "date1", to_date('date_string, "dd/MM-yyyy") as "date2").select(datediff('date1, 'date2)).show()
  //Ex1.2
//  val timeStampDf = Seq((1, "2020-01-15 15:04:58:865", "15-02-2020 45:50")).toDF("id", "ts", "ts_string")
//  timeStampDf.printSchema()
//  timeStampDf.show()
//  timeStampDf.select(to_timestamp('ts, "yyyy-MM-dd HH:mm:ss:SSS"), to_timestamp('ts_string, "dd-MM-yyyy mm:ss")).show()
  //Ex1.3
//  val dateStringDf = Seq("2020-02-14 05:35:55").toDF("date")
//  dateStringDf.select(to_date('date) as "date").select(year('date), month('date), dayofmonth('date), hour('date), minute('date), second('date), dayofweek('date), quarter('date)).show()
  //Ex1.4
  Seq("2020-02-14 05:35:55").toDF("date").select(date_format('date, "yyyyMMdd")).show()

}
