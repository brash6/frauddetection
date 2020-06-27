package spendreport

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class TimestampExtractor extends AssignerWithPeriodicWatermarks[ObjectNode] with Serializable {
  override def extractTimestamp(e: ObjectNode, prevElementTimestamp: Long) = {
    e.findValue("timestamp").asLong()*1000
  }
  override def getCurrentWatermark(): Watermark = {
    new Watermark(System.currentTimeMillis - 10000)
  }
}
