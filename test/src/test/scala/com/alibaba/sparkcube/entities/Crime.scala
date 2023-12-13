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

package com.alibaba.sparkcube.entities

case class Crime(
  INCIDENT_NUMBER: Option[String],
  OFFENSE_CODE: Option[Int],
  OFFENSE_CODE_GROUP: Option[String],
  OFFENSE_DESCRIPTION: Option[String],
  DISTRICT: Option[String],
  REPORTING_AREA: Option[String],
  SHOOTING: Option[String],
  OCCURRED_ON_DATE: Option[String],
  YEAR: Option[Int],
  MONTH: Option[Int],
  DAY_OF_WEEK: Option[String],
  HOUR: Option[Int],
  UCR_PART: Option[String],
  STREET: Option[String],
  Lat: Option[BigDecimal],
  Long: Option[BigDecimal],
  Location: Option[String])
