/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.sparkcube.optimizer

import com.alibaba.sparkcube.CubeManager
import com.alibaba.sparkcube.entities.Crime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

class TestCacheSuite extends org.scalatest.FunSuite {

  test("test using spark") {
    val crimeFilepath = getClass.getResource("/crime.csv").getPath
    val offenseCodesFilePath = getClass.getResource("/offense_codes.csv").getPath

    val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local[4]")
        .appName("test")
        .enableHiveSupport()
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.cache.tab.display", "true")
        .config("spark.sql.cache.useDatabase", "ct")
        .config("spark.sql.extensions", "com.alibaba.sparkcube.SparkCube")
        .config("spark.driver.extraClassPath",
          "/Users/tigrulya/IdeaProjects/SparkCube/sparkcube.jar")
        .config("spark.jars",
          "/Users/tigrulya/IdeaProjects/SparkCube/sparkcube.jar")
        .getOrCreate()
    }

    import spark.implicits._
    val crimes_csv = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeFilepath)
      .withColumn("lat", $"lat".cast(DecimalType(38, 18)))
      .withColumn("long", $"long".cast(DecimalType(38, 18)))
      .as[Crime]

    spark.sql("CREATE DATABASE IF NOT EXISTS ct")

    crimes_csv.write.mode(SaveMode.Overwrite)
      .format("csv")
      .saveAsTable("ct.crimes")

    //    val offense_codes_csv = spark.read
    //      .option("header", "true")
    //      .option("inferSchema", "true")
    //      .csv(offenseCodesFilePath)
    //      .as[OffenseCode]

    val otherTable = Seq("B2", "E13", "B3", "C7")
      .toDF("dist_id")

    otherTable.createOrReplaceTempView("secondTable")

    val cubeManager = new CubeManager()
    cubeManager.createCache(
      spark,
      "ct.crimes",
      CacheFormatInfo(
        cacheName = "test_cache",
        rewriteEnabled = true,
        provider = "ORC",
        cacheSchema = CacheCubeSchema(
          dims = Seq("DISTRICT", "OFFENSE_CODE_GROUP"),
          measures = Seq(Measure(
            column = "INCIDENT_NUMBER",
            func = "COUNT"
          ))
        )
      )
    )

    cubeManager.buildCache(
      spark,
      CacheIdentifier(
        db = "ct",
        viewName = "crimes",
        cacheName = "test_cache"
      )
    )

    val simpleQuery = "SELECT OFFENSE_CODE_GROUP, DISTRICT, COUNT(INCIDENT_NUMBER) as INCIDENTS " +
      "from CT.CRIMES where DISTRICT <> 'A7' group by OFFENSE_CODE_GROUP, DISTRICT"

    spark.sql(simpleQuery).collect()

    val query = "SELECT v.*  from secondTable join " +
      "(SELECT OFFENSE_CODE_GROUP, DISTRICT, COUNT(INCIDENT_NUMBER) as INCIDENTS " +
      "from CT.CRIMES where DISTRICT <> 'A7' group by OFFENSE_CODE_GROUP, DISTRICT) as v " +
      "on v.DISTRICT = secondTable.DIST_ID"

    val result = spark.sql(query).collect()

    spark.sql(query).collect()
    spark.sql(query).collect()
    spark.sql(query).collect()
    spark.sql(query).collect()
  }

}
