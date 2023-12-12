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

package org.apache.spark.sql

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.RewriteCacheQuerySuite.{TEST_DB, TMP_WAREHOUSE_DIR}
import org.apache.spark.sql.TestUtils.readPlanFromResources
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SQLTestUtilsBase

import com.alibaba.sparkcube.{CacheBuildUtils, SparkCube}
import com.alibaba.sparkcube.optimizer.{CacheCubeSchema, Measure}

class RewriteCacheQuerySuite extends AnyFunSuite
  with SQLTestUtilsBase
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with CacheBuildUtils {

  import testImplicits._

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    System.clearProperty(IS_TESTING.key)
    spark = SparkSession
      .builder()
      .master("local")
      .appName("test")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.cache.tab.display", "false")
      .config("spark.sql.cache.useDatabase", TEST_DB)
      .config(StaticSQLConf.WAREHOUSE_PATH.key, TMP_WAREHOUSE_DIR.getPath)
      .withExtensions(new SparkCube())
      .getOrCreate()

    CubeSharedState.setActiveState(new CubeSharedState(spark))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    try {
      spark.sessionState.catalog.reset()
    } finally {
      // TestUtils.deleteDirectory(TMP_WAREHOUSE_DIR)
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("test rewrite simple select") {
    val table = "simple_select"
    withDatabase(TEST_DB) {
      createTestTable(TEST_DB, table)
      createDefaultCache(TEST_DB, table)

      val actualPlan = sql(
        s"""
           |SELECT group, COUNT(num) AS num_count
           |FROM $TEST_DB.$table
           |GROUP BY group
           |""".stripMargin)
        .queryExecution
        .optimizedPlan

      val expectedPlan = readPlanFromResources("simple_select_optimized")
      assert(expectedPlan == actualPlan.treeString)
    }
  }

  test("test rewrite join with alias") {
    val table = "join_with_alias"
    withDatabase(TEST_DB) {
      createTestTable(TEST_DB, table)
      createDefaultCache(TEST_DB, table)

      Seq("group1", "group2")
        .toDF("group_name")
        .createOrReplaceTempView("groups")

      val actualPlan = sql(
        s"""
           |SELECT groups.group_name, cached_table.num_count
           |FROM groups join (
           |  SELECT group, COUNT(num) AS num_count
           |  FROM $TEST_DB.$table
           |  GROUP BY group
           |) as cached_table
           |ON cached_table.group = groups.group_name
           |""".stripMargin)
        .queryExecution
        .optimizedPlan

      val expectedPlan = readPlanFromResources("join_with_alias_optimized")
      assert(expectedPlan == actualPlan.treeString)
    }
  }

  test("test support projection on cache") {
    val table = "uppercase_select"
    withDatabase(TEST_DB) {
      createTestTable(TEST_DB, table)

      createAndBuildCache(
        db = TEST_DB,
        table = table,
        cacheSchema = CacheCubeSchema(
          dims = Seq("group"),
          measures = Seq(
            Measure(
              column = "num",
              func = "COUNT"
            ),
            // additional aggregation
            Measure(
              column = "id",
              func = "COUNT"
            ))
        )
      )

      val actualPlan = sql(
        s"""
           |SELECT group, COUNT(num) AS num_count
           |FROM $TEST_DB.$table
           |GROUP BY group
           |""".stripMargin)
        .queryExecution
        .optimizedPlan

      val expectedPlan = readPlanFromResources("projection_on_cache_optimized")
      assert(expectedPlan == actualPlan.treeString)
    }
  }

  // TODO support case insensitive column names
  ignore("test rewrite simple select with uppercase columns") {
    val table = "uppercase_select"
    withDatabase(TEST_DB) {
      createTestTable(TEST_DB, table)
      createDefaultCache(TEST_DB, table)

      val actualPlan = sql(
        s"""
           |SELECT GROUP, COUNT(NUM) AS num_count
           |FROM $TEST_DB.$table
           |GROUP BY group
           |""".stripMargin)
        .queryExecution
        .optimizedPlan

      val expectedPlan = readPlanFromResources("simple_select_optimized")
      assert(expectedPlan == actualPlan.treeString)
    }
  }

  private def createTestTable(db: String, table: String): Unit = {
    sql(s"CREATE DATABASE IF NOT EXISTS $db")

    Seq((1, "group1", 3))
      .toDF("id", "group", "num")
      .write
      .saveAsTable(s"$db.$table")
  }
}

object RewriteCacheQuerySuite {
  val TMP_WAREHOUSE_DIR = TestUtils.createTempDir("warehouse")
  val TEST_DB = "rewrite_test_db"
}