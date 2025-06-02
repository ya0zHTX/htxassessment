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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.github.ya0zHTX.htxassessment.model.GeoLocationRecord
import scala.collection.Map

// Service for handling geolocation data, specifically for broadcasting it.

class BroadcastGeolocation(sc: SparkContext) {

  /**
   * Collects geolocation records into a Map and broadcasts it.
   *
   * @param geoLocationRDD An RDD of GeoLocationRecord objects.
   * @return A Broadcast variable containing a Map from geographical_location_oid to geographical_location.
   */
  def broadcastGeoLocations(geoLocationRDD: RDD[GeoLocationRecord]): Broadcast[scala.collection.Map[Long, String]] = {
    val geolocationMap: scala.collection.Map[Long, String] = geoLocationRDD
      .map(l => (l.geographical_location_oid, l.geographical_location))
      .collectAsMap()

    sc.broadcast(geolocationMap)
  }
}

