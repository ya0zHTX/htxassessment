package com.github.ya0zHTX.htxassessment

import com.github.ya0zHTX.htxassessment.model._
import com.github.ya0zHTX.htxassessment.util.SparkSessionWrapper
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: Main <detectionsFilePath> <geolocationsFilePath> <outputFilePath> <topX>")
      sys.exit(1)
    }

    val Array(detectionsPath, geolocationsPath, outputPath, topXStr) = args
    val topX = topXStr.toInt
    val spark = SparkSessionWrapper.get("RDD Project")
    val sc = spark.sparkContext

    val dfDetections = spark.read.parquet(detectionsPath)
    val dfGeoLocations = spark.read.parquet(geolocationsPath)

    val detectionRDD = dfDetections.rdd
      .map(row => DetectionRecord(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[Long]("video_camera_oid"),
        row.getAs[Long]("detection_oid"),
        row.getAs[String]("item_name"),
        row.getAs[Long]("timestamp_detected")
      ))
      // Dedup key: (detection_oid, video_camera_oid, geographical_location_oid)
      .map(d => ((d.detection_oid, d.video_camera_oid, d.geographical_location_oid), d))
      .reduceByKey((a, _) => a) // keep one DetectionRecord per the unique keys above
      .map { case (_, d) => (d.geographical_location_oid, d) }
      // .map(d => (d.geographical_location_oid, d))
      // .distinct() // remove duplicate based on all of the fields

    val geolocationRDD = dfGeoLocations.rdd
      .map(row => GeoLocationRecord(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("geographical_location")
      ))
      .map(l => (l.geographical_location_oid, l.geographical_location))

    val cogrouped = detectionRDD.cogroup(geolocationRDD)

    val cogroupedRDD = cogrouped.flatMap {
      case (_, (dets, locs)) =>
        for {
          det <- dets
          loc <- locs
        } yield ((det.item_name, loc), 1)
    }

    val topItems = cogroupedRDD
      .reduceByKey(_ + _)
      .map { case ((item, _), count) => (item, count) }
      .reduceByKey(_ + _)
      .takeOrdered(topX)(Ordering[Int].reverse.on(_._2)) // only get the top X
      .map(_._1)
      .toSet

    val finalRDD = cogroupedRDD
      .reduceByKey(_ + _)
      .filter { case ((item, _), _) => topItems.contains(item) }
      .map { case ((item, location), count) => Row(item, location, count) }

    val outputSchema = StructType(Seq(
      StructField("item_name", StringType),
      StructField("geographical_location", StringType),
      StructField("detection_count", IntegerType)
    ))

    val resultDF = spark.createDataFrame(finalRDD, outputSchema)
    resultDF.write.mode(SaveMode.Overwrite).parquet(outputPath)


    val outputDF = spark.read.parquet(outputPath)

    // Show the contents in console
    outputDF.show(false)  // false to avoid truncation
    spark.stop()
  }
}
