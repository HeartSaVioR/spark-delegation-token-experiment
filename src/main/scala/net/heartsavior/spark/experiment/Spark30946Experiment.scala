package net.heartsavior.spark.experiment

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.FileStreamSinkLog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession

object Spark30946Experiment extends Logging {
  import Spark30946Util._

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println("Usage: Spark30946Experiment <root metadata path> <min metadata version>" +
        " <max metadata version> <batch ID to add> <entries per batch> <trial count>")
      System.exit(1)
    }

    val rootMetadataPath = args(0)
    val minMetadataVersion = args(1).toInt
    val maxMetadataVersion = args(2).toInt
    val targetBatchId = args(3).toInt
    val entriesPerBatch = args(4).toInt
    val trialCount = args(5).toInt

    logInfo(s"Root dir for metadata: $rootMetadataPath")
    logInfo(s"Min version of metadata: $minMetadataVersion")
    logInfo(s"Max version of metadata: $maxMetadataVersion")
    logInfo(s"Entries per batch: $entriesPerBatch")
    logInfo(s"batch ID to add: $targetBatchId")
    logInfo(s"Number of trials: $trialCount")

    val spark = SparkSession
      .builder
      .appName("SPARK-30946 preparation")
      .config(SQLConf.FILE_SINK_LOG_CLEANUP_DELAY.key, 0)
      .master("local[*]")
      .getOrCreate()

    (1 to trialCount).foreach { trial =>
      (minMetadataVersion to maxMetadataVersion).foreach { version =>
        logInfo(s"trial: $trial / version: $version / entries per batch:" +
          s" $entriesPerBatch / batch ID to add: $targetBatchId")

        withTempDir { tmpDir =>
          // Follows the naming convention of the directory for preparation
          val sourceDir = new File(rootMetadataPath, s"version-$version")

          logInfo("Start copying metadata files for isolated testing...")
          FileUtils.copyDirectory(sourceDir, tmpDir)
          logInfo("Copy done... run test...")

          val sinkLog = new FileStreamSinkLog(version, spark, tmpDir.getCanonicalPath)

          val entries = createDummySinkFileStatuses(
            new File(rootMetadataPath), entriesPerBatch)

          val (_, elapsedMs) = timeTakenMs {
            sinkLog.add(targetBatchId, entries)
          }

          val targetFile = tmpDir.listFiles().find {
            _.getName.startsWith(targetBatchId.toString)
          }.get

          logInfo(s"trial: $trialCount / version: $version - took $elapsedMs ms," +
            s" target batch file size: ${targetFile.length()}")
        }

        // wait a bit
        Thread.sleep(1000)
      }
    }

    logInfo("Done...")
  }
}
