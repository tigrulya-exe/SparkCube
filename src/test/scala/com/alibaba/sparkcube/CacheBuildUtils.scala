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

package com.alibaba.sparkcube

import org.apache.spark.sql.SparkSession

import com.alibaba.sparkcube.optimizer.{CacheCubeSchema, CacheFormatInfo, CacheIdentifier, Measure}

trait CacheBuildUtils {
  protected val cubeManager: CubeManager = new CubeManager()

  protected def spark: SparkSession

  def createDefaultCache(db: String, table: String): Unit = {
    createAndBuildCache(
      db = db,
      table = table,
      cacheSchema = CacheCubeSchema(
        dims = Seq("group"),
        measures = Seq(Measure(
          column = "num",
          func = "COUNT"
        ))
      )
    )
  }

  def createAndBuildCache(db: String, table: String, cacheSchema: CacheCubeSchema): Unit = {
    val cacheName = s"${table}_cache"

    cubeManager.createCache(spark, s"$db.$table", CacheFormatInfo(
      cacheName = cacheName,
      provider = "ORC",
      cacheSchema = cacheSchema))

    cubeManager.buildCache(spark, CacheIdentifier(
      db = db,
      viewName = table,
      cacheName = cacheName
    ))
  }
}
