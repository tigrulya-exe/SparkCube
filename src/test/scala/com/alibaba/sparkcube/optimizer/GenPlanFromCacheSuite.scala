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

import java.io.File

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.TestUtils.readPlanFromResources
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.dsl.expressions.{count, DslAttr, DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, ReCountDistinct, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.sql.types._

import com.alibaba.sparkcube.CacheBuildUtils
import com.alibaba.sparkcube.optimizer.GenPlanFromCacheSuite.TMP_WAREHOUSE_DIR

class GenPlanFromCacheSuite extends SparkFunSuite
  with SharedSparkSessionBase
  with CacheBuildUtils {

  class TestExecutor(sparkSession: SparkSession) extends RuleExecutor[LogicalPlan] {
    val batches: Seq[Batch] = Batch("GenPlanFromCache", Once,
      GenPlanFromCache(sparkSession)) :: Nil
  }

  override protected def afterEach(): Unit = {
    try {
      spark.sessionState.catalog.reset()
    } finally {
      TestUtils.deleteDirectory(TMP_WAREHOUSE_DIR)
      super.afterEach()
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(WAREHOUSE_PATH.key, TMP_WAREHOUSE_DIR.getPath)
  }

  test("test getDataType") {
    val op = GenPlanFromCache(SparkSession.builder().master("local").getOrCreate())
    assert(op.getDataType("count", IntegerType) == LongType)
    assert(op.getDataType("COUNT", StringType) == LongType)
    assert(op.getDataType("Count", DateType) == LongType)
    assert(op.getDataType("MIN", ShortType) == ShortType)
    assert(op.getDataType("Max", ByteType) == ByteType)
    assert(op.getDataType("pre_count_distinct", BooleanType) == BinaryType)
    assert(op.getDataType("AVG", LongType) == DoubleType)
    assert(op.getDataType("Sum", IntegerType) == LongType)
    assert(op.getDataType("Sum", ByteType) == LongType)
    assert(op.getDataType(
      "Sum", SparkAgent.createDecimal(10, 2)) == SparkAgent.createDecimal(20, 2))
    assert(op.getDataType("Sum", FloatType) == DoubleType)
  }

  test("test buildAggrFunc") {
    val op = GenPlanFromCache(SparkSession.builder().master("local").getOrCreate())
    val exp = BoundReference(0, IntegerType, nullable = true)
    assert(op.buildAggrFunc(Seq(exp), "Count", promo = true).isInstanceOf[Sum])
    assert(op.buildAggrFunc(Seq(exp), "count", promo = false).isInstanceOf[Count])
    assert(op.buildAggrFunc(Seq(exp), "SUM", promo = false).isInstanceOf[Sum])
    assert(op.buildAggrFunc(Seq(exp), "pre_count_distinct", promo = false).isInstanceOf[Count])
    assert(op.buildAggrFunc(
      Seq(exp), "pre_count_distinct", promo = true).isInstanceOf[ReCountDistinct])
  }

  test("test rewrite simple select") {
    val table = "simple_select"
    withTempDatabase { testDb =>
      withSQLConf("spark.sql.cache.useDatabase" -> testDb) {
        val relation = createTestTable(testDb, table)

        createDefaultCache(testDb, table)

        val logicalPlan = relation.select($"id", $"group", $"num")
          .groupBy($"group")(count($"num").as("num_count"))
          .analyze

        val optimizedPlan = new TestExecutor(spark).execute(logicalPlan)
        val expectedPlan = readPlanFromResources("simple_select")

        assert(expectedPlan == optimizedPlan.treeString)
      }
    }
  }

  test("test rewrite join with alias") {
    val table = "join_with_alias"
    withTempDatabase { testDb =>
      withSQLConf("spark.sql.cache.useDatabase" -> testDb) {
        val relation = createTestTable(testDb, table)

        createDefaultCache(testDb, table)

        val aggregation = relation
          .select($"id", $"group", $"num")
          .groupBy($"group")($"group", count($"num").as("num_count"))
          .subquery(Symbol("cached_table"))

        val secondTable = LocalRelation($"join_id".int)

        val join = secondTable
          .select($"join_id")
          .join(aggregation, Inner, Some($"join_id" === $"cached_table.group"))
          .analyze

        val optimizedPlan = new TestExecutor(spark).execute(join)
        val expectedPlan = readPlanFromResources("join_with_alias")

        assert(expectedPlan == optimizedPlan.treeString)
      }
    }
  }

  private def createTestTable(db: String, table: String): LogicalRelation = {
    val localRelation = LocalRelation(
      $"id".int,
      $"group".string,
      $"num".int
    )

    val catalogTable = createDummyCatalogTable(
      TableIdentifier(table, Some(db)),
      localRelation.schema
    )
    val plan = CreateTable(catalogTable, SaveMode.Ignore, None)
    spark.sessionState.executePlan(plan).executedPlan.execute()

    LogicalRelation(
      TestRelation(spark.sqlContext, localRelation.schema),
      catalogTable
    )
  }

  private def createDummyCatalogTable(id: TableIdentifier, schema: StructType): CatalogTable = {
    CatalogTable(
      identifier = id,
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map()
      ),
      provider = Some("parquet"),
      schema = schema
    )
  }

  case class TestRelation(sqlContext: SQLContext, schema: StructType) extends BaseRelation
}

object GenPlanFromCacheSuite {
  val TMP_WAREHOUSE_DIR: File = TestUtils.createTempDir("warehouse")
}