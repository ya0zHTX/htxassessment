import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object GenerateUnitTestData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Generate Test Data")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val detections = Seq(
      Row(1001L, 5001L, 9001L, "Item_1", 1717000000000L),
      Row(1001L, 5001L, 9002L, "Item_2", 1717000000010L),
      Row(1001L, 5002L, 9001L, "Item_1", 1717000000020L),
      Row(1001L, 5002L, 9001L, "Item_1", 1717000000020L),
      Row(1002L, 5003L, 9003L, "Item_3", 1717000000030L),
      Row(1002L, 5004L, 9004L, "Item_3", 1717000000040L), 
      Row(1002L, 5004L, 9005L, "Item_4", 1717000000050L),
      Row(1002L, 5004L, 9004L, "Item_3", 1717000000060L), // Duplicate to test
      Row(1002L, 5004L, 9004L, "Item_3", 1717000000070L), // Duplicate to test
      Row(1002L, 5004L, 9006L, "Item_4", 1717000000070L),
      Row(1002L, 5004L, 9007L, "Item_4", 1717000000080L)
    )

    val detectionSchema = StructType(Seq(
      StructField("geographical_location_oid", LongType),
      StructField("video_camera_oid", LongType),
      StructField("detection_oid", LongType),
      StructField("item_name", StringType),
      StructField("timestamp_detected", LongType)
    ))

    val detectionDF = spark.createDataFrame(
      spark.sparkContext.parallelize(detections),
      detectionSchema
    )

    detectionDF.write.mode("overwrite").parquet("data/detections.parquet")

    val locations = Seq(
      Row(1001L, "Location_1"),
      Row(1002L, "Location_2")
    )

    val locationSchema = StructType(Seq(
      StructField("geographical_location_oid", LongType),
      StructField("geographical_location", StringType)
    ))

    val locationDF = spark.createDataFrame(
      spark.sparkContext.parallelize(locations),
      locationSchema
    )

    locationDF.write.mode("overwrite").parquet("data/geolocations.parquet")

    spark.stop()
  }
}