import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.util.parsing.json.JSON

object Main extends App {
  val hdfsPath = config.hdfs.hdfs()
  val spark = config.spark.spark()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(1))

  import spark.implicits._

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "10.60.158.53:9290",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("signing_invoice_cluster1")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  
  val invoiceStream = stream.map(record => (record.key, record.value))
    .map(x => JSON.parseFull(x._2).getOrElse().asInstanceOf[Map[String, String]])
    .map(x => x.get("objectValue").getOrElse().asInstanceOf[Map[String, String]])
    .map(x => (x.get("invoiceNo").getOrElse().asInstanceOf[String],
      x.get("invoiceSeri").getOrElse().asInstanceOf[String],
      x.get("issueDate").getOrElse().asInstanceOf[String],
      x.get("transactionId").getOrElse().asInstanceOf[String],
      x.get("totalAmountWithVAT").getOrElse().asInstanceOf[Double]
    ))
  //Ex1.1
//  invoiceStream.foreachRDD { rdd: RDD[(String, String, String, String, Double)] =>
//    val invoice = rdd.toDF("invoiceNo", "invoiceSeri", "issueDate", "transactionId", "totalAmountWithVAT")
//    invoice.createOrReplaceTempView("invoice")
//    spark.sql("select * from invoice").show()
//  }
  //Ex1.2
  val invoiceType = sc.parallelize(Seq(("EA/20E", "ETC"), ("MA/20E", "MTC"))).toDF("invoiceSeri", "Type")
  invoiceType.createOrReplaceTempView("invoiceType")
  invoiceStream.foreachRDD { rdd: RDD[(String, String, String, String, Double)] =>
    val invoice = rdd.toDF("invoiceNo", "invoiceSeri", "issueDate", "transactionId", "totalAmountWithVAT")
    invoice.createOrReplaceTempView("invoice")
    spark.sql("select * from invoice join invoiceType on invoice.invoiceSeri = invoiceType.invoiceSeri").show()
  }


  ssc.start()
  ssc.awaitTermination()

}
