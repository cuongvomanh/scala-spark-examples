package config

import org.apache.spark.sql.SparkSession

object spark {

  def spark(): SparkSession = {
    val sparkSessionBuilder = SparkSession
      .builder
      .appName("MnMCount")
    if (sys.env("SPARK_MASTER") == "local"){
      sparkSessionBuilder.config("spark.master", "local")
    }
    val spark = sparkSessionBuilder
      .getOrCreate()
    return spark;
  }
}
