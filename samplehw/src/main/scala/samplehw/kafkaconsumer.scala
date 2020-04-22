package samplehw

import org.apache.spark.sql.SparkSession;

object kafkaconsumer {
  
    def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
        
    val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "importJobComplete").load
    val modelTempsQuery = kafkaDF.writeStream.outputMode("update").format("console").option("truncate","false").start()
    }
}