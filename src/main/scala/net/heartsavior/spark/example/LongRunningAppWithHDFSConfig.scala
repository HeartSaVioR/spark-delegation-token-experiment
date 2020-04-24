package net.heartsavior.spark.example

import scala.io.{Codec, Source}

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

/**
 * This example reads the config from HDFS "before" initializing Spark Session
 * (as well as SparkContext) to validate Spark can handle credentials properly
 * in specific mode. (e.g. yarn-client vs yarn-cluster, keytab vs ticket cache, etc.)
 */
object LongRunningAppWithHDFSConfig extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LongRunningAppWithHDFSConfig <HDFS config file path> <checkpoint location>")
      System.exit(1)
    }

    val hdfsConfigFilePath = args(0)
    val checkpointPath = args(1)

    logInfo(s"Config file path: $hdfsConfigFilePath")
    logInfo(s"Checkpoint path: $checkpointPath")

    val fs = FileSystem.get(SparkHadoopUtil.get.conf)
    val is = fs.open(new Path(hdfsConfigFilePath))
    val groupModRange = try {
      Source.fromInputStream(is)(Codec.UTF8).getLines().next().toInt
    } finally {
      is.close()
    }

    logInfo(s"key range: ${0 until groupModRange}")

    val spark = SparkSession
      .builder
      .appName("StructuredStreamingHDFSConfig")
      .getOrCreate()

    import org.apache.spark.sql.functions._

    val input = spark.readStream
      .format("rate")
      .load()

    val df = input
      .selectExpr(s"MOD(value, $groupModRange) AS key", "value")
      .groupBy("key")
      .agg(count("value"))

    val query = df.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointPath)
      .start()

    query.awaitTermination()
  }
}