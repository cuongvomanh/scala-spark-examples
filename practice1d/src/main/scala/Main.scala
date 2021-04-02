import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  // Push file in README.md
  // Ex1
  val rddAirportPath = hdfsPath + "/dgd2/sparkcore/in/3/airports.text"
  val rddAirport = sc.textFile(rddAirportPath)
//  rddAirport.foreach(println)
  // Preprocessing
  val attrs = Array("Airport ID", "Name of airport", "Main city served by airport", "Country where airport is located", "IATA/FAA code", "ICAO Code", "Latitude", "Longitude", "Altitude", "Timezone", "DST", "Timezone in Olson format")
  val types = Array("String", "String", "String", "String", "String", "String", "Float", "String", "String", "String", "String", "String")
  val preprocessRddAirport = rddAirport.map(x => x.replaceAll("(^[0-9]*,\"[^\"]*\",\"[^\"]*),([^\"]*\")", "$1_$2")).map(x => x.split(",").map(x => x.trim)).map(x=> (x(0), x(1), x(2), x(3), x(4), x(5), x(6).toFloat, x(7)))
  // Ex1.1
//  val latitudeGreaterThan40 = preprocessRddAirport.filter(x => x._7 > 40).map(x => (x._2, x._7))
//  latitudeGreaterThan40.saveAsTextFile(hdfsPath + "/dgd2/sparkcore/out/task1a")
  // Ex1.2
  preprocessRddAirport.filter(x => x._2.contains(x._3)).map(x => (x._4, 1)).reduceByKey((a,b) => a +b).foreach(println)
  // Ex2
  val apacheRdd = sc.textFile(hdfsPath + "/dgd2/sparkcore/in/4/")
  // host	logname	time	method	url	response	bytes
  apacheRdd.map(x => x.split("\\s+")).map(x => (x(0), x(2), x(3), x(5))).filter(x => x._3 == "GET").filter(x => x._4 == "200").map(x => (if (x._1.matches("[0-9\\.]*")) "IP REQUEST" else "DOMAIN REQUEST", DateTimeFormatter.ofPattern("yyyyMM").withZone(ZoneId.systemDefault()).format(Instant.ofEpochSecond(x._2.toInt)), x._3, x._4)).map(x => ((x._1, x._2), 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, x._1._2, x._2)).foreach(println)
}
