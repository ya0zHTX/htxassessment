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

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import com.github.ya0zHTX.htxassessment.model.DetectionRecord
import com.github.ya0zHTX.htxassessment.model.OutputRecord
import scala.collection.Map

// Service for processing detection data, joining with geolocations data, counting items, and ranking items per location.

class RankingItem {

  /**
   * Processes detection data to calculate top items per geographical location.
   *
   * @param detectionRDD The RDD of (geographical_location_oid, DetectionRecord) pairs.
   * @param broadcastedGeolocationMap The broadcasted map of geographical locations.
   * @param topX The number of top items to consider per location.
   * @return An RDD of OutputRecord objects.
   */
  def calculateTopRankedItems(
      detectionRDD: RDD[(Long, DetectionRecord)],
      broadcastedGeolocationMap: Broadcast[scala.collection.Map[Long, String]],
      topX: Int
  ): RDD[OutputRecord] = {
    // 1. Join detectionRDD with the broadcasted geolocation map and count item occurrences.
    // 2. For each detection record in detectionRDD, look up its geographical_location using the broadcasted geolocation map.
    // 3. Create pairs of ((item_name, geographical_location), count).
    val itemLocationCountsRDD = detectionRDD.flatMap { case (geo_oid, det) =>
      broadcastedGeolocationMap.value.get(geo_oid) match {
        case Some(location) => Some(((det.item_name, location), 1))
        case None => None // If location not found, skip this record
      }
    }.reduceByKey(_ + _) // Sum up counts for each unique (item_name, geographical_location) pair.

    // 4. Group by geographical location and calculate top X items per location.
    itemLocationCountsRDD
      .map { case ((item, location), count) => (location, (item, count)) } // Map to (geographical_location, (item_name, count)) to group by location.
      .groupByKey() // Group all items and their counts by geographical_location.
      .flatMap { case (location, itemsWithCounts) =>
        itemsWithCounts.toList
          .sortBy(-_._2) // Sort by count in descending order
          .take(topX)    // Take only the top X items for this location
          .zipWithIndex  // Add a 0-based index (rank)
          .map { case ((item, _), index) =>
            OutputRecord(location, (index + 1).toString, item) // Create an OutputRecord for each top item, converting rank to String as per the question.
          }
      }
  }
}

