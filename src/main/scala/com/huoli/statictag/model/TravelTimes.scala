package com.huoli.statictag.model

import com.huoli.statictag.model.ResidentCityNew.hsc
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  *
  * 出行的频次
  *
  */
object TravelTimes {

  private var hsc:HiveContext=null

  def main(args:Array[String]): Unit ={

    val conf=new  SparkConf()
    conf.setMaster("local[20]").setAppName("test-count")
    val sc=new SparkContext(conf)
    hsc=new HiveContext(sc)

   var sql1=
     """
       |select t1.user_id,count(1)
       |  from s_airline_order t1, s_airline_order_detail t2, s_user_info u
       | where t2.dt = t1.dt
       |   and t1.order_id = t2.order_id
       |   and t1.user_id=u.user_id
       |   and t1.orderstatue IN ('5', '52', '53', '71', '73', '74', '75')
       |   and (u.passport = t2.passenger_uid OR u.id_card = t2.passenger_uid)
       |group by t1.user_id
     """.stripMargin

   /* sql1=
      """
        |select t1.user_id,t2.order_id,concat_ws('|', collect_list(t2.passenger_uid)) passengers from  s_airline_order t1,s_airline_order_detail t2  where t1.user_id='xsZUJWXf39I=' and t1.orderstatue IN ('5', '52', '53', '71', '73', '74', '75')  and t1.order_id=t2.order_id group by t1.user_id,t2.order_id
      """.stripMargin*/

    val df=hsc.sql(sql1)

    df.rdd.saveAsTextFile("D:\\zxy\\tempdir\\user_travel_count8")

    sc.stop()

  }

}
