object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  // Push file in README.md

  // Ex1
//  val rddFilePath = hdfsPath + "/dgd2/sparkcore/in/1/data.txt"
//  val rddFile = sc.textFile(rddFilePath)

  // Ex1.1
//  rddFile.foreach(println)
//  println(rddFile.count())
//  rddFile.take(3).foreach(println)
  // Ex1.2
//  rddFile.map(x => x.length).foreach(println)
  // Ex1.3
//  rddFile.map(x => x.toUpperCase).foreach(println)
  // Ex1.4
//  rddFile.flatMap(x => x.split(" ")).foreach(println)
  // Ex1.5
//  rddFile.flatMap(x => x.split(" ")).map(x=>x.replaceAll("[,\\.]","")).map(x => (x,1)).reduceByKey((a,b)=> a + b).foreach(println)

  // Ex 2
  val dataVideo = hdfsPath + "/dgd2/sparkcore/in/2/"
  val rddVideo = sc.textFile(dataVideo)
  // Ex2.1
//  rddVideo.foreach(println)
  // Ex2.2
  rddVideo.map(x =>(x.split("\\|")(0), x.split("\\|")(1), x.split("\\|")(2))).filter(x => x._2=="Gaming").map(x => (x._1,x._3)).reduceByKey((a, b) => a + b).foreach(println)
}
