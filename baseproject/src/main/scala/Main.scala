object Main extends App{
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val hdfsPath = config.hdfs.hdfs()
  // Push file in README.md
  // Ex1
//  val rddFilePath = hdfsPath + "/dgd2/sparkcore/in/1/data.txt"
//  val rddFile = sc.textFile(rddFilePath)
}
