# HTX Assessment
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

```
runMain GenerateUnitTestData
```

**Test data:**
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
