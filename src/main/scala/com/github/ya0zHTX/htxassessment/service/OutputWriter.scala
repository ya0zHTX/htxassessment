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

import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.rdd.RDD
import com.github.ya0zHTX.htxassessment.model.OutputRecord

// Service for writing output data to a Parquet file.

class OutputWriter(spark: SparkSession) {

  /**
   * Writes an RDD of OutputRecord to a Parquet file.
   *
   * @param outputRecordsRDD The RDD of OutputRecord objects to write.
   * @param outputPath The path where the Parquet file will be written.
   */
  def writeOutput(outputRecordsRDD: RDD[OutputRecord], outputPath: String): Unit = {
    // Define the schema for the output DataFrame using the OutputRecord structure.
    val outputSchema = StructType(Seq(
      StructField("geographical_location", StringType),
      StructField("item_rank", StringType),
      StructField("item_name", StringType)
    ))

    // Convert the RDD of OutputRecord to an RDD of Row with the above schema.
    val rowRDD = outputRecordsRDD.map(outputRecord =>
      Row(outputRecord.geographical_location, outputRecord.item_rank, outputRecord.item_name)
    )

    // Create a DataFrame from the RDD of Row with the above schema.
    val resultDF = spark.createDataFrame(rowRDD, outputSchema)

    // Write the result DataFrame to a Parquet file, overwriting if it already exists.
    resultDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}

