package com.github.ya0zHTX.htxassessment.util
import org.apache.spark.sql.SparkSession

object SparkSessionWrapper {
  def get(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
