package com.huoli.statictag.model

import java.util.Date

import com.huoli.statictag.model.ResidentCityNew._
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  *
  * 出行的频次统计划分
  *
  * 统计一年的数据 月更新
  *
  * auth  zengxy
  *
  * create date 2018-08-06
  *
  */
object TravelTimes {

  private var hsc:HiveContext=null
  private  val logger:Logger=Logger.getLogger(this.getClass)
  private  val  dateFormat:FastDateFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
    *
    */
  def threeYearSelfOrder: Unit ={
    val sql1=
      """
        |select o.user_id, o.order_id, passenger_uid, dt
        |  from (select order_id, user_id, dt
        |          from s_airline_order
        |         where orderstatue IN ('5', '52', '53', '71', '73', '74', '75')
        |           and dt >= add_months(CURRENT_TIMESTAMP, -36)) o
        | INNER JOIN (select order_id, sex, birthday, passenger_uid
        |               from s_airline_order_detail
        |              where dt >= add_months(CURRENT_TIMESTAMP, -36)) d
        |    on o.order_id = d.order_id
      """.stripMargin

    hsc.sql(sql1).write.mode(SaveMode.Overwrite).format("orc").saveAsTable("tmp.three_year_order_id")

    val sql2=
      """
        |select d.user_id, d.order_id, count(1) travel_time, d.dt
        |  from (select user_id, order_id, passenger_uid,dt from tmp.three_year_order_id) d
        |  join s_user_info u
        |    on (u.user_id = d.user_id and
        |       (u.passport = d.passenger_uid OR u.id_card = d.passenger_uid))
        | group by d.user_id, d.order_id, d.dt
      """.stripMargin

    hsc.sql(sql2).write.mode(SaveMode.Overwrite).format("orc").saveAsTable("tmp.three_year_myself_order_id")


    val sql3=
      """
        |create table   hbdb.three_year_myself_order_detail as
        |select o.order_id,
        |       o.user_id,
        |       d.sex,
        |       d.birthday,
        |       d.passenger_uid,
        |       d.dep_time,
        |       d.create_time,
        |       d.dep_code,
        |       d.arr_code,
        |       d.arr_country_type,
        |       d.dep_country_type,
        |       d.dep_city_name,
        |       d.arr_city_name,
        |       m.dt
        |  from (select order_id, user_id
        |          from s_airline_order
        |         where orderstatue IN ('5', '52', '53', '71', '73', '74', '75')
        |           and dt >= add_months(CURRENT_TIMESTAMP, -36)) o
        | INNER JOIN (select order_id,
        |                    sex,
        |                    birthday,
        |                    passenger_uid,
        |                    dep_time,
        |                    create_time,
        |                    dep_code,
        |                    arr_code,
        |                    arr_country_type,
        |                    dep_country_type,
        |                    dep_city_name,
        |                    arr_city_name
        |               from s_airline_order_detail
        |              where dt >= add_months(CURRENT_TIMESTAMP, -36)) d
        |    on o.order_id = d.order_id
        | INNER JOIN tmp.three_year_myself_order_id m
        |    on o.order_id = m.order_id
      """.stripMargin

    hsc.sql(" drop table if exists hbdb.three_year_myself_order_detail ")
    hsc.sql(sql3)
  }


  /***
    *  统计用户出行次数    一年的数据
    *  更新周期           月
    */
  def oneYearTravelTime() = {
     val sql1=
       """
         |create table tmp.user_travel_type as
         |select t.user_id,
         |       (case
         |         when t.travel_times < 5 then
         |          '低频'
         |         when t.travel_times < 15 then
         |          '普通'
         |         when t.travel_times < 25 then
         |          '高频'
         |         else
         |          '超高频'
         |       end) travel_type
         |  from (select user_id, sum(travel_time) travel_times
         |          from tmp.three_year_myself_order_id
         |         where dt >= add_months(CURRENT_TIMESTAMP, -12)
         |         group by user_id) t
       """.stripMargin

    hsc.sql(" drop table if exists tmp.user_travel_type ")
    hsc.sql(sql1)
  }

  def main(args:Array[String]): Unit ={

    logger.info(" begin to analysis user's data : "+dateFormat.format(new Date()))

    val conf=new  SparkConf()
   // conf.setMaster("local[20]").setAppName("test-count")
    val sc=new SparkContext(conf)
    hsc=new HiveContext(sc)
    logger.info(" begin to analysis 3 year user self travel order : "+dateFormat.format(new Date()))
    threeYearSelfOrder
    logger.info(" begin to analysis 1 year user self travel  time : "+dateFormat.format(new Date()))
    oneYearTravelTime
    logger.info(" 分析程序结束数据已入库 : "+dateFormat.format(new Date()))
    sc.stop()
  }
}
