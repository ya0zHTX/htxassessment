# HTX Assessment
## Assumptions

1. Assumed that you are using a MacOS machine when setting up the environment
2. The ranking is ranked based on the `geographical_location` and not just displaying the top X items across all `geographical_location`
3. The scalastyle checks does not throw errors and warnings are based on the default style configured in `scalastyle-config.xml`
4. Scalastyle gives warning for `println` due to not using a logger.
5. Logger is not used in this code and errors will be thrown out to the console.
6. Deduplication of the detections dataset is based on the keys `(detection_oid, video_camera_oid, geographical_location_oid)`

## Setup environment to run Scala
### Install JDK
```
brew install openjdk@11
```
### Link the Java Runtime

**For Apple Silicon**

```
sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
```

**For non Apple Silicon (Intel chip)**

```
sudo ln -sfn /usr/local/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
```

Add `JAVA_HOME` to your shell profile (Optional)

```
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Install Scala
Run the following command to install Scala
```
brew install coursier/formulas/coursier && cs setup
```
### Install sbt
```
brew install sbt
```

### Start the `sbt` server
Run the command `sbt` to start the sbt server and install the necessary packages. After everything is setup, a prompt that looks like below will appear:

```
sbt:HTXAssessment>
```

### Generate unit test data or more test data
Run the command in the `sbt` terminal:

**Unit test data:**
The unit testing is done for deduplication of the detections dataset based on the keys `(detection_oid, video_camera_oid, geographical_location_oid)`

```
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
```
Expected result:
```
+---------------------+---------+---------+
|geographical_location|item_rank|item_name|
+---------------------+---------+---------+
|Location_1           |1        |Item_1   |
|Location_1           |2        |Item_2   |
|Location_2           |1        |Item_4   |
|Location_2           |2        |Item_3   |
+---------------------+---------+---------+
```

**Generate unit test data:**

```
runMain GenerateUnitTestData
```

**To generate larger test datasets:**
```
runMain GenerateTestData
```

### Run the main program
Run the command in the `sbt` terminal e.g. 

`runMain com.github.ya0zHTX.htxassessment.Main <DetectionsFilePath> <GeoLocationFilePath> <outputPath/outputFile> <topX>`

```
runMain com.github.ya0zHTX.htxassessment.Main ./data/detections.parquet ./data/geolocations.parquet ./data/output.parquet 10
```

### To ScalaStyle check
Run the command in `sbt` terminal to run the plugin `scalastyle`

```
scalastyle
```

## Git repository structure **NOT required**
Used `sbt` to create a Scala Park code repository.

Directory structure
```
htxassessment/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/
│       │       └── github/
│       │          └── ya0zHTX/
│       │               └── htxassessment/
│       │                   ├── model/
│       │                   │   ├── DetectionRecord.scala
│       │                   │   └── LocationRecord.scala
│       │                   ├── service/
│       │                   │   ├── BroadcastGeolocation.scala
│       │                   │   ├── OutputWriter.scala
│       │                   │   ├── ParquetLoader.scala
│       │                   │   ├── RankingItem.scala
│       │                   ├── util/
│       │                   │   └── SparkSessionWrapper.scala
│       │                   ├── Main.scala
├── data/
│   ├── detections.parquet
│   └── locations.parquet
├── GenerateTestData.scala
├── GenerateUnitTestData.scala
├── README.md
├── scalastyle-config.xml
├── .gitignore
└── .sbtopts
```
