package org.apache.spark

import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.SparkHadoopUtil

object SparkHadoopUtilHack {
  def conf(): Configuration = {
    SparkHadoopUtil.get.conf
  }
}
