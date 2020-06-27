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
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

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
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.util.Collector
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

      val fraudulent_ctr_uid_detection = AnomalyDetection.fraudulent_ctr_uid(ctr_compute_uid, 0.2, 3)

      val sink_ctr_uid: StreamingFileSink[(String, String, String, Double, String, Int)] = StreamingFileSink
        .forRowFormat(new Path("fraudulent_ctr_uid_detection"), new SimpleStringEncoder[(String, String, String, Double, String, Int)]("UTF-8"))
        .withRollingPolicy(
          DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
            .withMaxPartSize(1024*1024*1024)
            .build())
        .build();

      fraudulent_ctr_uid_detection.addSink(sink_ctr_uid);

      """Fraudulent CTR IP"""

      val click_and_displays_ip = stream_events.map(node => (node.findValue("ip").asText(), node.findValue("eventType").asText()))
        .map(value => (value._1 ,value._2))


      val ctr_compute_ip = AnomalyDetection.ctr_ip(click_and_displays_ip, 300, 60)

      val fraudulent_ctr_ip_detection = AnomalyDetection.fraudulent_ctr_ip(ctr_compute_ip, 0.2, 3)

      val sink_ctr_ip: StreamingFileSink[(String, String, String, Double, String, Int)] = StreamingFileSink
        .forRowFormat(new Path("fraudulent_ctr_ip_detection"), new SimpleStringEncoder[(String, String, String, Double, String, Int)]("UTF-8"))
        .withRollingPolicy(
          DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
            .withMaxPartSize(1024*1024*1024)
            .build())
        .build();

      fraudulent_ctr_ip_detection.addSink(sink_ctr_ip);



      """Fraudulent mean time ImpressionId per uid"""

      val impressionId_timestamps_per_id = stream_events.map(node => (node.findValue("uid").asText(), node.findValue("impressionId").asText(), node.findValue("timestamp").asInt()))
        .map(value => (value._1 ,value._2, value._3))

      val impressionid_tmstp_per_uid = AnomalyDetection.fraud_impressionId_tmstp(impressionId_timestamps_per_id, 1, 60, 30)

      val sink_impressionid_tmstp_per_uid: StreamingFileSink[(String, String, String, Double)] = StreamingFileSink
        .forRowFormat(new Path("fraudulent_impressionid_tmstp_per_uid"), new SimpleStringEncoder[(String, String, String, Double)]("UTF-8"))
        .withRollingPolicy(
          DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
            .withMaxPartSize(1024*1024*1024)
            .build())
        .build();

      impressionid_tmstp_per_uid.addSink(sink_impressionid_tmstp_per_uid);

      """Fraudulent mean time ImpressionId per ip"""

      val impressionId_timestamps_per_ip = stream_events.map(node => (node.findValue("ip").asText(), node.findValue("impressionId").asText(), node.findValue("timestamp").asInt()))
        .map(value => (value._1 ,value._2, value._3))

      val impressionid_tmstp_per_ip = AnomalyDetection.fraud_impressionId_per_ip_tmstp(impressionId_timestamps_per_ip, 1, 60, 30)

      val sink_impressionid_tmstp_per_ip: StreamingFileSink[(String, String, String, Double)] = StreamingFileSink
        .forRowFormat(new Path("fraudulent_impressionid_tmstp_per_ip"), new SimpleStringEncoder[(String, String, String, Double)]("UTF-8"))
        .withRollingPolicy(
          DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
            .withMaxPartSize(1024*1024*1024)
            .build())
        .build();

      impressionid_tmstp_per_ip.addSink(sink_impressionid_tmstp_per_ip);







      env.execute("Fraud Detection")


  }
}

