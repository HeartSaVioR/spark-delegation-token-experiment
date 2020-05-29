package net.heartsavior.spark.experiment

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.FileStreamSinkLog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object Spark30946Preparation extends Logging {
  import Spark30946Util._

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: Spark30946Preparation <root metadata path> <min metadata version>" +
        " <max metadata version> <last batch ID to prepare> <entries per batch>")
      System.exit(1)
    }

    val rootMetadataPath = args(0)
    val minMetadataVersion = args(1).toInt
    val maxMetadataVersion = args(2).toInt
    val lastBatchIdToPrepare = args(3).toInt
    val entriesPerBatch = args(4).toInt

    logInfo(s"Root dir for metadata: $rootMetadataPath")
    logInfo(s"Min version of metadata: $minMetadataVersion")
    logInfo(s"Max version of metadata: $maxMetadataVersion")
    logInfo(s"Entries per batch: $entriesPerBatch")
    logInfo(s"last batch ID to prepare: $lastBatchIdToPrepare")

    val spark = SparkSession
      .builder
      .appName("SPARK-30946 preparation")
      .config(SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key, 0)
      .master("local[*]")
      .getOrCreate()

    val sinkLogs = (minMetadataVersion to maxMetadataVersion).map { version =>
      val dir = new File(rootMetadataPath, s"version-$version")
      logWarning(s"Use directory $dir for version $version")
      new FileStreamSinkLog(version, spark, dir.getAbsolutePath)
    }

    val dirAsFile = new File(rootMetadataPath)
    (0 to lastBatchIdToPrepare).foreach { batchId =>
      val entries = createDummySinkFileStatuses(dirAsFile, entriesPerBatch)
      sinkLogs.foreach { _.add(batchId, entries) }
    }

    logWarning("Done...")
  }

}
