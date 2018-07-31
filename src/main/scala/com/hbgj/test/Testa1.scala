package com.hbgj.test

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object Testa1 {
  def main(args: Array[String]): Unit = {



    System.setProperty("SPARK_LOCAL_DIRS","D:\\zxy\\worknote\\201807\\thread_p1\\GroovyJscala\\src\\main\\resources\\dir")
    val conf=new  SparkConf()

    conf.setMaster("local[8]").setAppName("test-1")

    val sc=new SparkContext(conf)


    val hsc=new HiveContext(sc)

    var sql=
      """
        |select *
 |  from (select phoneid, city, event_time, event_date
 |          from sp_class.areaby_latlnt
 |         where event_date >= '2017-02-22'
 |           and phoneid != 'NULL'
 |         order by phoneid, event_time) s
 |  left join (select o.user_id,
 |                    collect_list(concat(d.dep_time,
 |                                        '|',
 |                                        d.dep_city_name,
 |                                        ',',
 |                                        d.arr_city_name)) as ticketslist
 |               from s_airline_order_detail d
 |               join s_airline_order o
 |                 on (d.order_id = o.order_id and
 |                    o.orderstatue IN
 |                    ('5', '52', '53', '71', '73', '74', '75') and
 |                    d.dep_time >= '2017-02-20 00:00:00')
 |              group by o.user_id) u
 |    on s.phoneid = u.user_id

      """.stripMargin

/*    sql=
      """
        |select * from s_airline_order_detail d limit 10
        |
        |
      """.stripMargin*/

    val df=hsc.sql(sql)





    /**
      * 上面的sql分为两个部分
      *
      * select phoneid, city, event_time, event_date
      * from sp_class.areaby_latlnt
      * where event_date >= '2017-02-22'
      * and phoneid != 'NULL'
      * order by phoneid, event_time
      *
      * 第一个部分是查询  用户经纬度区域活跃表(sp_class.areaby_latlnt)
      *
      * select o.user_id , collect_list(concat(d.dep_time,
      * '|',
      *                                         d.dep_city_name,
      * ',',
      *                                         d.arr_city_name)) as ticketslist
      * from s_airline_order_detail d
      * join s_airline_order o
      * on (d.order_id = o.order_id and
      *                     o.orderstatue IN
      * ('5', '52', '53', '71', '73', '74', '75') and
      *                     d.dep_time >= '2017-02-20 00:00:00')
      * join s_user_info u
      * on (u.user_id = o.user_id)
      * where u.id_card = d.passenger_uid
      * or u.passport = d.passenger_uid
      * group by o.user_id) temp
      * 第二个部分是查询在某段时期内 , 成功订购过机票,绑定过身份证或护照的用户id
      *
      *
      * 将这两部分的结果左关联
      *
      *
      *
      */



    //df.rdd.saveAsTextFile("D:\\zxy\\tempdir\\temp.txt")

    scala.sys.addShutdownHook({

      println("HOOK IS RUNNING .......... JobUtils.TaskKill:")
    })


    val detailloc = df.rdd

    var pre_city:String = null

    var rrd  = detailloc.map( r => {
      //手机id
      val phoneid = r.getAs[String]("phoneid")
      //活跃城市
      val city = r.getAs[String]("city")
      //活跃时间
      val etime: Timestamp = r.getAs[Timestamp]("event_time")
      //所有行程
      val ticketslist = r.getAs[Seq[String]]("ticketslist")

      /**
        * 如果行程不为空=>
        *
        *
        *
        * 否则 =>
        */
      if (ticketslist != null ) {

        var flag=0

        //可以再进行精确一步，后期可优化
        ticketslist.foreach(r => {
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val t = format.parse(r.split('|').head);
          val order_citys:Array[String] = r.split('|').last.split(',')
          var t_0 = format.parse(r.split('|').head);
          t_0.setTime(t.getTime+3600000)

          /**
            * 如果活跃时间 小于出行时间  活跃城市 为出发城市 flag置为1
            * 如果活跃实际 大于出行时间  活跃城市 为到达城市 flag置为1
            */
          if (etime.before(t_0) && city.equals(order_citys.head)) {
            flag = 1;
          }else if(etime.after(t) && city.equals(order_citys.last)){
            flag = 1;
          }

        })






        if(pre_city==null || !pre_city.equals(city)){
          pre_city = city
          Row(phoneid,city,etime, flag)
        }else{
          Row()
        }

      } else {
        if(pre_city==null || !pre_city.equals(city)) {
          pre_city = city
          //(phoneid, city, etime, false)
          Row(phoneid, city, etime, 0)
        }else{
          Row()
        }
      }

    })



    //过滤掉空白数据
    rrd  = rrd.filter( x => x!=Row() )


    rrd.saveAsTextFile("D:\\zxy\\tempdir\\rdd")






    val schema = StructType(
      Array[StructField](
        StructField("phoneid", StringType) ,
        StructField("city", StringType),
        StructField("etime", TimestampType),
        StructField("flag", IntegerType)
      )
    )

    var loc_df=hsc.createDataFrame(rrd, schema)

    loc_df.registerTempTable("sp_class_loc_phoneid")

    val querySql2=
      """
        |select phoneid as user_id, city, sum(rate) * 0.5 as rate
        |  from (select a.phoneid, city, flag, 1 / allcnt as rate
        |          from sp_class_loc_phoneid a
        |          join (select phoneid, count(1) as allcnt
        |                 from sp_class_loc_phoneid
        |                group by phoneid) t
        |            on a.phoneid = t.phoneid
        |         where flag = 0) u
        | group by phoneid, city
      """.stripMargin

    var loc_ret_df = hsc.sql(querySql2)

    hsc.sql("drop table if exists sp_class.df_tmp_latlnt_phoneid")

    loc_ret_df.saveAsTable("sp_class.df_tmp_latlnt_phoneid")

    hsc.dropTempTable("sp_class_loc_phoneid")




    sc.stop()



  }
}
