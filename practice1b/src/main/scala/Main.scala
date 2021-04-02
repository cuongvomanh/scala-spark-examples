object Main extends App{
  val spark = config.spark.spark();
  val sc = spark.sparkContext

  // Ex
  val fruits = Array("Peach", "Orange", "Apple")
  val frdd = sc.parallelize(fruits)
  // Ex1
//  println(frdd.getNumPartitions)
  // Ex2
  val hdfsPath = config.hdfs.hdfs();
  val fruitsPath = hdfsPath + "/dgd2/sparkcore/out/1b"
  // Ex2.1
//  frdd.saveAsTextFile(fruitsPath);
  // Ex2.2
  val frddr = sc.textFile(fruitsPath)
  println(frddr.collect().foreach(println))
  println(frddr.getNumPartitions)

}
