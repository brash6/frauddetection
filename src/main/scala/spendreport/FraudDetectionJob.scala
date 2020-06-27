/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction

import scala.collection.JavaConverters._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.api.scala.{DataSet, createTypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
  * Skeleton code for the DataStream code walkthrough
  */
object FraudDetectionJob {

    def main(args: Array[String]){

        val env = StreamExecutionEnvironment.getExecutionEnvironment

       env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


      val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181")
        properties.setProperty("group.id", "test")



      val streamedData = new FlinkKafkaConsumer[ObjectNode](List("clicks","displays").asJava, new JSONKeyValueDeserializationSchema(false), properties)
      val stream_events = env.addSource(streamedData).assignTimestampsAndWatermarks(new TimestampExtractor)


      """Fraudulent CTR UID"""

      val click_and_displays_uid = stream_events.map(node => (node.findValue("uid").asText(), node.findValue("eventType").asText()))
        .map(value => (value._1 ,value._2))

      val ctr_compute_uid = AnomalyDetection.ctr_uid(click_and_displays_uid, 60, 30)

      val fraudulent_ctr_uid_detection = AnomalyDetection.fraudulent_ctr_uid(ctr_compute_uid, 0.2, 3).print()

      """Fraudulent CTR IP"""

      val click_and_displays_ip = stream_events.map(node => (node.findValue("ip").asText(), node.findValue("eventType").asText()))
        .map(value => (value._1 ,value._2))


      val ctr_compute_ip = AnomalyDetection.ctr_ip(click_and_displays_ip, 300, 60)

      val fraudulent_ctr_ip_detection = AnomalyDetection.fraudulent_ctr_ip(ctr_compute_ip, 0.05, 3).print()


      """Fraudulent Timestamp

      val uid_timestamps = stream_events.map(node => (node.findValue("uid").asText(), node.findValue("eventType").asText(), node.findValue("timestamp").asInt()))
        .map(value => (value._1 ,value._2, value._3))

      val fraudulent_timestamp_uid_detection = AnomalyDetection.fraudulent_timestamp_per_uid(uid_timestamps, 1, 60, 30).print()
      """

      """Fraudulent mean time ImpressionId"""

      val impressionId_timestamps = stream_events.map(node => (node.findValue("uid").asText(), node.findValue("impressionId").asText(), node.findValue("timestamp").asInt()))
        .map(value => (value._1 ,value._2, value._3))

      val impressionid_tmstp = AnomalyDetection.fraud_impressionId_tmstp(impressionId_timestamps, 1, 60, 30).print()








      env.execute("Fraud Detection")


  }
}

