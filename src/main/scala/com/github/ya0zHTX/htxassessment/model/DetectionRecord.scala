
package com.github.ya0zHTX.htxassessment.model

case class DetectionRecord(
  geographical_location_oid: Long,
  video_camera_oid: Long,
  detection_oid: Long,
  item_name: String,
  timestamp_detected: Long
)
