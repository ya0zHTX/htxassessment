// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.github.ya0zHTX.htxassessment.service

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import com.github.ya0zHTX.htxassessment.model.{DetectionRecord, GeoLocationRecord}

// Service for loading detection and geolocation data from the Parquet files and performing initial transformations.

class ParquetLoader(spark: SparkSession) {

  /**
   * Loads detection records from a Parquet file and removed duplicates.
   * The deduplication key is (detection_oid, video_camera_oid, geographical_location_oid).
   *
   * @param detectionsPath The path to the detections Parquet file.
   * @return An RDD of (geographical_location_oid, DetectionRecord) pairs.
   */
  def loadDetections(detectionsPath: String): RDD[(Long, DetectionRecord)] = {
    val dfDetections = spark.read.parquet(detectionsPath)
    dfDetections.rdd
      .map(row => DetectionRecord(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[Long]("video_camera_oid"),
        row.getAs[Long]("detection_oid"),
        row.getAs[String]("item_name"),
        row.getAs[Long]("timestamp_detected")
      ))
      // Dedup key: (detection_oid, video_camera_oid, geographical_location_oid)
      .map(d => ((d.detection_oid, d.video_camera_oid, d.geographical_location_oid), d))
      .reduceByKey((a, _) => a) // Keep one DetectionRecord per the unique keys
      .map { case (_, d) => (d.geographical_location_oid, d) } // Map to (geographical_location_oid, DetectionRecord)
  }

  /**
   * Loads geolocation records from a Parquet file.
   *
   * @param geolocationsPath The path to the geolocations Parquet file.
   * @return An RDD of GeoLocationRecord objects.
   */
  def loadGeoLocations(geolocationsPath: String): RDD[GeoLocationRecord] = {
    val dfGeoLocations = spark.read.parquet(geolocationsPath)
    dfGeoLocations.rdd
      .map(row => GeoLocationRecord(
        row.getAs[Long]("geographical_location_oid"),
        row.getAs[String]("geographical_location")
      ))
  }
}

