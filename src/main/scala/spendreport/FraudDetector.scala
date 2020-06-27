package spendreport

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class FraudDetector extends ProcessWindowFunction[(String, Int, Int), (String,Int, Int), String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Int, Int)],
                       out: Collector[(String, Int, Int)]): Unit = {

    var difference = 0
    var sum_difference = 0
    var nb_clicks = 0
    var last_tmsp = 0
    elements.foreach( value => {
      val timestamp = value._2
      nb_clicks += value._3
      if(last_tmsp == 0){
        last_tmsp = timestamp
        difference = last_tmsp
      }
      val current_uid = value._1
      if(!difference.equals(timestamp)) {
        difference = timestamp - last_tmsp
        sum_difference += difference
      }
      if(nb_clicks == elements.size - 1){
        out.collect((value._1, sum_difference/nb_clicks, nb_clicks))
      }

    })
  }
}