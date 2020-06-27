package spendreport

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object AnomalyDetection {

    def isClick(eventType : String) : Int ={
      if (eventType == "click")
      {
        1
      }
      else{
        0
      }
    }

    def click_t_r(value1 : Int, value2 : Int): Double={
      if (value2>0){
        value1.toFloat/value2
      }
      else{
        value2
      }
    }

    def ctr_uid(dst : DataStream[(String, String)], window_size : Int, window_slide : Int) : DataStream[(String, Int, Int,  Double)] = {
      val click_through_rate = dst
        .map(value => (value._1, isClick(value._2),1 - isClick(value._2)))
        .keyBy(0)
        .timeWindow(Time.seconds(window_size), Time.seconds(window_slide))
        .reduce((v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + v2._3))
        .map(e => (e._1, e._2, e._3, click_t_r(e._2,e._3)))
      click_through_rate
    }

  def ctr_ip(dst : DataStream[(String, String)], window_size : Int, window_slide : Int) : DataStream[(String, Int, Int,  Double)] = {
    val click_through_rate_ip = dst
      .map(value => (value._1, isClick(value._2),1 - isClick(value._2)))
      .keyBy(0)
      .timeWindow(Time.seconds(window_size), Time.seconds(window_slide))
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + v2._3))
      .map(e => (e._1, e._2, e._3, click_t_r(e._2,e._3)))
    click_through_rate_ip
  }

  def fraudulent_ctr_uid(dst : DataStream[(String, Int, Int, Double)], ctr_threshold : Double, nb_click_threshold : Int) : DataStream[(String, String, String, Double, String, Int)] = {
    val fraudulent_click_t_r = dst
      .filter(value => value._4 > ctr_threshold)
      .filter(value => value._2 > nb_click_threshold)
      .filter (value => value._2 < value._3)
        .map(value => ("Fraudulent UID", value._1, "CTR", value._4, "NB_clicks", value._2))
    fraudulent_click_t_r
  }

  def fraudulent_ctr_ip(dst : DataStream[(String, Int, Int, Double)], ctr_threshold : Double, nb_click_threshold : Int) : DataStream[(String, String, String, Double, String, Int)] = {
    val fraudulent_click_t_r_ip = dst
      .filter(value => value._4 > ctr_threshold)
      .filter(value => value._2 > nb_click_threshold)
      .filter (value => value._2 < value._3)
      .map(value => ("Fraudulent IP", value._1, "CTR", value._4, "NB_clicks", value._2))
    fraudulent_click_t_r_ip
  }

 """ def fraudulent_timestamp_per_uid(dst : DataStream[(String, String, Int)], mean_timestamp_threshold : Double, window_size : Int, window_slide : Int) : DataStream[(String, String, String, Double)] = {
    val fraudulent_tmstp = dst
        .filter(value => value._2 == "click")
      .map(value => (value._1, value._3, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(window_size), Time.seconds(window_slide))
      .process(new FraudDetector())
      .filter(value => value._2 < mean_timestamp_threshold)
      .map(value => ("Fraudulent uid (Timestamp)", value._1, "mean time between each click :", value._2))
    fraudulent_tmstp
  }"""

  def fraud_impressionId_tmstp(dst : DataStream[(String, String, Int)], difference_timestamp_threshold : Double, window_size : Int, window_slide : Int) : DataStream[(String, String, String, Double)] = {
    val fraud_impressionId = dst
      .map(value => (value._1, value._2, value._3, 1))
      .keyBy(1)
      .timeWindow(Time.seconds(window_size), Time.seconds(window_slide))
      .reduce((v1,v2) => (v1._1, v1._2, (v1._3 - v2._3).abs, v1._4 + v2._4))
      .filter(value => value._4 == 2)
      .map(value => (value._1, value._3, 1))
      .keyBy(0)
      .reduce((v1,v2) => (v1._1, v1._2 + v2._2, v1._3 + v2._3))
      .map(value => (value._1, click_t_r(value._2, value._3)))
    .filter(value => value._2 < difference_timestamp_threshold)
      .map(value => ("Fraudulent uid (Timestamp between click and display)", value._1, "Mean Time between click and display", value._2))
    fraud_impressionId
  }

  def fraud_impressionId_per_ip_tmstp(dst : DataStream[(String, String, Int)], difference_timestamp_threshold : Double, window_size : Int, window_slide : Int) : DataStream[(String, String, String, Double)] = {
    val fraud_impressionId = dst
      .map(value => (value._1, value._2, value._3, 1))
      .keyBy(1)
      .timeWindow(Time.seconds(window_size), Time.seconds(window_slide))
      .reduce((v1,v2) => (v1._1, v1._2, (v1._3 - v2._3).abs, v1._4 + v2._4))
      .filter(value => value._4 == 2)
      .map(value => (value._1, value._3, 1))
      .keyBy(0)
      .reduce((v1,v2) => (v1._1, v1._2 + v2._2, v1._3 + v2._3))
      .map(value => (value._1, click_t_r(value._2, value._3)))
      .filter(value => value._2 < difference_timestamp_threshold)
      .map(value => ("Fraudulent ip (Timestamp between click and display)", value._1, "Mean Time between click and display", value._2))
    fraud_impressionId
  }
}
