package spendreport

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class cityChangeCheck extends ProcessWindowFunction[(String, String, Int), (String,String, Int), String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String, Int)],
                       out: Collector[(String, String, Int)]): Unit = {

    var CTR = 0
    val last_uid = ""
    elements.foreach( value => {
      val event = value._2.toLowerCase
      val current_uid = value._1
      if(event == "click"){
        val nb_click = value._3
      }
      else {
        val nb_display = value._3
      }
      if (current_uid==last_uid){

      }
      if(event != "click"){
        out.collect(("___Alarm____", s"${value} marked for FREQUENT city changes", 0))
      }

    })
  }
}