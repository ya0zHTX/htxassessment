# HTX Assessment
## Create a public git repository
Using `sbt` to create a Scala Park code repository.

Directory structure
```
spark-rdd-clean-template/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/
│       │       └── github/
|       |           └── ya0zHTX/
|       |               └── htxassessment/
│       │                   ├── Main.scala
│       │                   ├── model/
│       │                   │   ├── DetectionRecord.scala
│       │                   │   └── LocationRecord.scala
│       │                   └── util/
│       │                       └── SparkSessionWrapper.scala
├── data/
│   ├── detections.parquet
│   └── locations.parquet
├── README.md
└── .gitignore
```
