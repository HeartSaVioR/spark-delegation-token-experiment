package net.heartsavior.spark.experiment

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.util.Random

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.execution.streaming.SinkFileStatus

object Spark30946Util {
  def createDummySinkFileStatuses(dir: File, count: Int): Array[SinkFileStatus] = {
    (1 to count).map { _ =>
      val size = Random.nextInt(2000000000)
      new SinkFileStatus(
        new File(dir, UUID.randomUUID().toString).getAbsolutePath,
        size,
        isDir = false,
        System.currentTimeMillis(),
        3,
        size,
        "add"  // ADD_ACTION
      )
    }.toArray
  }

  def withTempDir(f: File => Unit): Unit = {
    val systemTmpDir = FileUtils.getTempDirectory
    val tmpDir = new File(systemTmpDir, UUID.randomUUID().toString)
    try f(tmpDir) finally {
      FileUtils.forceDelete(tmpDir)
    }
  }

  def timeTakenMs[T](body: => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    (result, math.max(NANOSECONDS.toMillis(endTime - startTime), 0))
  }
}
