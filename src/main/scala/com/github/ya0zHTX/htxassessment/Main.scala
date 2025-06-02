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
package com.github.ya0zHTX.htxassessment

import com.github.ya0zHTX.htxassessment.util.SparkSessionWrapper
import com.github.ya0zHTX.htxassessment.service.{ParquetLoader, BroadcastGeolocation, RankingItem, OutputWriter}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) { // Ensure that exactly four command-line arguments are provided.
      println("Usage: Main <detectionsFilePath> <geolocationsFilePath> <outputFilePath> <topX>")
    }

    // Unpack the command-line arguments into respective variables.
    val Array(detectionsPath, geolocationsPath, outputPath, topXStr) = args
    // Convert the topX string argument to an integer.
    val topX = topXStr.toInt

    // Initialize SparkSession
    val spark = SparkSessionWrapper.get("HTX Assessment")
    val sc = spark.sparkContext // Get SparkContext for broadcasting

    try {
      // Initialize the services
      val dataLoader = new ParquetLoader(spark)
      val broadcastGeolocation = new BroadcastGeolocation(sc)
      val rankingItem = new RankingItem()
      val outputWriter = new OutputWriter(spark)

      // Load Data
      println("--- Loading detection and geolocation data ---")
      val detectionRDD = dataLoader.loadDetections(detectionsPath)
      val geoLocationRDD = dataLoader.loadGeoLocations(geolocationsPath)

      // Broadcast GeoLocations
      println("--- Broadcasting geolocation data map ---")
      val broadcastedGeolocationMap = broadcastGeolocation.broadcastGeoLocations(geoLocationRDD)

      // Calculate Top Ranked Items
      println(s"--- Calculating top $topX ranked items per geographical location ---")
      val finalOutputRDD = rankingItem.calculateTopRankedItems(
        detectionRDD,
        broadcastedGeolocationMap,
        topX
      )

      // Write Output
      println("--- Writing final output data ---")
      outputWriter.writeOutput(finalOutputRDD, outputPath)
      // Show the contents of the output DataFrame for verification
      println("Output Data:")
      spark.read.parquet(outputPath).show(false)
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop() // Stop the SparkSession
      println("SparkSession stopped. Program finished.")
    }
  }
}
