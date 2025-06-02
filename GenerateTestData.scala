import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import org.apache.commons.io.FileUtils

case class DetectionRecord(
  geographical_location_oid: Long,
  video_camera_oid: Long,
  detection_oid: Long,
  item_name: String,
  timestamp_detected: Long
)

case class GeoLocationRecord(
  geographical_location_oid: Long,
  geographical_location: String
)

object GenerateTestData {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession in local mode
    val spark = SparkSession.builder()
      .appName("Generate Test Parquet Data")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val baseOutputPath = "data"
    val detectionsOutputPath = s"$baseOutputPath/detections.parquet"
    val geolocationsOutputPath = s"$baseOutputPath/geolocations.parquet"

    // Clean up previous test data directories if they exist
    println(s"Cleaning up existing test data at $baseOutputPath...")
    val outputDir = new File(baseOutputPath)
    if (outputDir.exists()) {
      FileUtils.deleteDirectory(outputDir)
      println("Previous test data cleaned.")
    } else {
      println("No previous test data found.")
    }


    // --- Generate GeoLocationRecord Data ---
    println("\n--- Generating GeoLocation Data ---")
    val numGeoLocations = 10000 // Set the number of geolocation rows
    val geoLocations = (1 to numGeoLocations).map { i =>
      GeoLocationRecord(
        geographical_location_oid = 100000L + i,
        geographical_location = s"Location_${i}"
      )
    }

    val dfGeoLocations = geoLocations.toDF()
    dfGeoLocations.write.mode("overwrite").parquet(geolocationsOutputPath)
    println(s"Generated ${dfGeoLocations.count()} geolocations data at: $geolocationsOutputPath")


    // --- Generate DetectionRecord Data ---
    println("\n--- Generating Detection Data ---")
    val records = new scala.collection.mutable.ListBuffer[DetectionRecord]()
    val numDistinctItems = 20 
    val baseTimestamp = System.currentTimeMillis()

    // Generate a number of detections for ALL geographical locations
    val allGeoOids = geoLocations.map(_.geographical_location_oid)
    val numDetectionsPerLocation = 500
    allGeoOids.foreach { geoOid =>
      for (i <- 1 to numDetectionsPerLocation) {
        val itemId = (i % numDistinctItems) + 1
        records += DetectionRecord(
          geographical_location_oid = geoOid,
          video_camera_oid = (geoOid * 1000 + i % 200).toLong,
          detection_oid = (geoOid * 1000000 + i).toLong,
          item_name = s"Item_${itemId}",
          timestamp_detected = baseTimestamp + i * 1000L
        )
      }
    }

    val dfDetections = records.toDF()
    dfDetections.write.mode("overwrite").parquet(detectionsOutputPath)
    println(s"Generated detections data at: $detectionsOutputPath")

    spark.stop()
    println("SparkSession stopped. Test data generation complete.")
  }
}

