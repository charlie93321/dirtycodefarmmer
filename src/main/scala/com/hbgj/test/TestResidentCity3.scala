package com.hbgj.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TestResidentCity2 {


  def run(ssqlcontext:HiveContext,sc:SparkContext): Unit ={

    System.setProperty("HADOOP_USER_NAME", "hive")

    /**
      * 活跃经纬度区域               -----------       权重
      * 常用手机号归属地             -----------       权重
      * 订单行程(出发地-目的地)       -----------       权重
      *
      * 发票地址                     -----------       权重
      *
      *
      */
    generateAllLatlnt(ssqlcontext)


    generateContactPhone(ssqlcontext)


   /* val sql1=
      """
        |select u.user_id,
        |      d.dep_city_name,
        |      d.arr_city_name
        | from s_airline_order_detail d
        | join s_airline_order o
        |   on (d.order_id = o.order_id and
        |      o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75'))
        | join s_user_info u
        |   on (u.user_id = o.user_id and
        |      (u.passport = d.passenger_uid OR u.id_card = d.passenger_uid))
      """.stripMargin


    val df:DataFrame = ssqlcontext.sql(sql1)



    df.registerTempTable("sp_class_tmp_user_orders")*/







    ssqlcontext.sql(" drop table if exists sp_class.tmp_user_city_list")

    ssqlcontext.sql(" create table if not exists sp_class.tmp_user_city_list (user_id string, city string, cnt int) ")


    val sql2=
      """
        |insert into sp_class.tmp_user_city_list
        |  select user_id, city, count(1) as cnt
        |    from (select user_id,
        |                 explode(split(concat_ws(',',
        |                                         dep_city_name,
        |                                         arr_city_name),
        |                               ',')) as city
        |            from sp_class_tmp_user_orders) t
        |   group by user_id, city
      """.stripMargin

    ssqlcontext.sql(sql2)

    /**
      * 得到用户的全量数据
      */
    val sql3=
      """
        | select COALESCE(s.user_id, t.user_id) as user_id,
        |        case
        |          when s.phone_city is null then
        |           ''
        |          else
        |           s.phone_city
        |        end as phone_city,
        |        case
        |          when t.city is null then
        |           ''
        |          else
        |           t.city
        |        end as city,
        |        case
        |          when t.rate is null then
        |           0
        |          else
        |           t.rate
        |        end as rate
        |   from (select user_id, phone_city
        |           from s_user_info
        |          where phone_city != 'NULL') s
        |   full outer join (select a.user_id, a.city, a.cnt / allcnt as rate
        |                      from sp_class.tmp_user_city_list a
        |                      join (select user_id, sum(cnt) as allcnt
        |                             from sp_class.tmp_user_city_list
        |                            group by user_id) b
        |                        on a.user_id = b.user_id) t
        |     on s.user_id = t.user_id
      """.stripMargin


    ssqlcontext.sql(sql3).registerTempTable("sp_class_tmp_user_order_list")



    //for test
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_sp_class_tmp_user_order_list")



    ssqlcontext.sql("create table sp_class.df_tmp_sp_class_tmp_user_order_list  as select * from sp_class_tmp_user_order_list")




    generateInvoice(ssqlcontext,sc)






    val sql4=
      """
        | select user_id, city, rate
        |   from sp_class_tmp_user_order_list
        |  where city != ''
        | union all
        | select user_id, phone_city as city, 1 as rate
        |   from s_user_info
        |  where phone_city != 'NULL'
      """.stripMargin


    val df_sp = ssqlcontext.sql(sql4)

    val loc_ret_df = ssqlcontext.table("sp_class.df_tmp_latlnt_phoneid")




    val df_invoice = ssqlcontext.sql("select user_id, city, rate*1.2  as rate   from sp_class.df_tmp_invoice")


    var df_rret = df_sp.unionAll(df_invoice).unionAll(loc_ret_df)


    val  rdd_tmp = df_rret.map(r => {

         val key= ( r.getAs[String]("user_id"),  r.getAs[String]("city") )
         (key, r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (x) => {
           Row(x._1._1, x._1._2, x._2)
    })



    val invoice_schema = StructType(
      StructField("user_id", StringType) ::
        StructField("city", StringType ) ::
        StructField("rate", DoubleType)
        :: Nil)

    val  df_tmp = ssqlcontext.createDataFrame(rdd_tmp, invoice_schema)




    val contact_userinfo_df = ssqlcontext.table("sp_class.tmp_contact_phone_df")




    df_rret = df_tmp.unionAll(contact_userinfo_df)
    ssqlcontext.dropTempTable("sp_class_tmp_user_order_list")
    ssqlcontext.sql("drop table if exists sp_class.resident_city")


    val rdd_ret = df_rret.map(r => {
        val key=(r.getAs[String]("user_id"), r.getAs[String]("city"))
        (key, r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (X) => {
         Row(X._1._1, X._1._2, X._2)
    })


    val df_ret = ssqlcontext.createDataFrame(rdd_ret, invoice_schema)


    df_ret.registerTempTable("sp_class_tmp_city_rate")

    ssqlcontext.sql("drop table if exists sp_class.resident_city_001")

    val sql5=
      """
        | create table sp_class.resident_city_001 as
        | select user_id,
        |        city,
        |        rate,
        |        row_number() over(partition by user_id order by rate desc) rn
        |   from sp_class_tmp_city_rate
        |
      """.stripMargin

    ssqlcontext.sql(sql5)

    ssqlcontext.sql("create table sp_class.resident_city as select user_id,city,rate,  rn from sp_class.resident_city_001 where rn=1 ")


  }
  def main(args: Array[String]): Unit = {
    val conf=new  SparkConf()

    conf.setMaster("local[18]").setAppName("test-1")

    val sc=new SparkContext(conf)


    val hsc=new HiveContext(sc)


     run(hsc,sc)






   //generateAllLatlnt(hsc)

    sc.stop()
  }


  def generateAllLatlnt(ssqlcontext:HiveContext): Unit ={

    System.setProperty("HADOOP_USER_NAME", "hive")



    ssqlcontext.sql("drop table if exists sp_class.tmp_areaby_latlnt_resident ")


    /**
      * 建表
      * sp_class.tmp_areaby_latlnt_resident   活跃经纬度区域临时表
      *
      * 数据来源 sp_class.areaby_latlnt 活跃经纬度区域表
      * phoneid     手机id
      * city        活跃城市
      * event_date  活跃日期
      */
    val sql1=
      """
        | create table sp_class.tmp_areaby_latlnt_resident as
        | select phoneid, city, event_date
        |   from sp_class.areaby_latlnt
        |  where event_date >= '2017-02-22'
        |    and phoneid != 'NULL'
        |  group by phoneid, city, event_date
      """.stripMargin

    ssqlcontext.sql(sql1)

    /**
      * 来源  sp_class.tmp_areaby_latlnt_resident   活跃经纬度区域临时表
      * 同一个手机id 总共有多少条记录
      * select phoneid, count(1) as allcnt  from sp_class.tmp_areaby_latlnt_resident  group by phoneid
      * 同一个手机id ,同一个活跃城市 总共有多少条记录
      * select phoneid, city, count(1) as cnt from sp_class.tmp_areaby_latlnt_resident where city is not null and city != "" group by phoneid, city
      *
      * 他们的比值*0.5  就是 这个个用户的 在  某个活跃城市的 活跃度
      *
      */
    val sql2=
      """
        |  select a.phoneid as user_id, a.city, a.cnt / t.allcnt * 0.5 as rate
        |    from (select phoneid, city, count(1) as cnt
        |            from sp_class.tmp_areaby_latlnt_resident
        |           where city is not null
        |             and city != ''
        |           group by phoneid, city) a
        |    join (select phoneid, count(1) as allcnt
        |            from sp_class.tmp_areaby_latlnt_resident
        |           group by phoneid) t
        |      on a.phoneid = t.phoneid
      """.stripMargin



    val loc_ret_df = ssqlcontext.sql(sql2)

    ssqlcontext.sql("drop table if exists sp_class.df_tmp_latlnt_phoneid")

    loc_ret_df.saveAsTable("sp_class.df_tmp_latlnt_phoneid")

    ssqlcontext.sql("drop table if exists sp_class.tmp_areaby_latlnt_resident ")



  }












  def generateContactPhone(ssqlcontext:HiveContext):Unit ={

    /**
      *
      * s_airline_order   s_airline_order （主订单表）：default  航班订单表
      * user_id          |   手机id
      * order_id         |   订单id
      * orderstatue      |   订单状态
      * invoice_address  |   发票寄送地址
      * contact_phone	   |	 联系电话
      *
      * 订单状态： 0临时 1预订成功 11预订失败 12订单作废 2未支付 21支付待确认
      * 3支付成功 31支付失败 4待出票 41抢票中 5出票成功 51出票失败 52客票已使用
      * 53行程单已邮寄 6退票处理中 61已退票，待退款 62退款处理中 63已退票，已退款
      * 64退票失败 65退款失败 7改升处理中 71已改升 72改升支付待确认 73改升支付成功 74部分已改升 75全部已改升
      *
      * ['5'(出票成功), '52'(客票已使用), '53'(行程单已邮寄), '71'(已改升), '73'(改升支付成功), '74'(部分已改升), '75'(全部已改升)]
      *
      *s_airline_order_detail  航班订单详情表
      * order_id        |      订单id
      * passenger_uid   |      身份证号或护照(id_card|passport)
      * dep_time        |      出发时间
      * dep_city_name   |      出发城市
      * arr_city_name   |      到达城市
      *
      *
      *
      * s_user_info
      * user_id    |  手机id
      * passport   |   护照
      * id_card    |   身份证
      *
      * 订票者 的所有行程  (其中已剔除  帮别人订票的别人的行程 )
      */
    val sql1=
      """
        | select u.user_id,
        |        o.contact_phone,
        |        d.dep_city_name,
        |        d.arr_city_name,
        |        invoice_address
        |   from s_airline_order_detail d
        |   join s_airline_order o
        |     on (d.order_id = o.order_id and
        |        o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75'))
        |   join s_user_info u
        |     on (u.user_id = o.user_id and
        |        (u.passport = d.passenger_uid OR u.id_card = d.passenger_uid))
      """.stripMargin

    val df:DataFrame = ssqlcontext.sql(sql1)

    df.registerTempTable("sp_class_tmp_user_orders")
    //缺少电话联系方式的人员，加入订单内的联系方式


    /**
      *default.phone_location  手机归属地查询
      */
    val sql2=
      """
        |select u.user_id, l.city, u.phone_prefix, u.rate
        |  from (select a.user_id,
        |               substr(a.contact_phone, 0, 7) as phone_prefix,
        |               a.cnt / t.allcnt as rate
        |          from (select user_id, contact_phone, count(1) cnt
        |                  from sp_class_tmp_user_orders
        |                 group by user_id, contact_phone) a
        |          join (select user_id, count(1) as allcnt
        |                 from sp_class_tmp_user_orders
        |                group by user_id) t
        |            on a.user_id = t.user_id) u
        |  join default.phone_location l
        |    on u.phone_prefix = l.phone_number
      """.stripMargin

    val contact_phone_df = ssqlcontext.sql(sql2)

    contact_phone_df.registerTempTable("sp_class_tmp_contact_phone_df")

    /**
      *
      */
    val sql3=
      """
        |select a.user_id, a.city, a.rate
        |  from sp_class_tmp_contact_phone_df a
        |  join s_user_info s
        |    on a.user_id = s.user_id
        | where s.phone_city is null
        |
      """.stripMargin

    val contact_userinfo_df = ssqlcontext.sql(sql3)


    ssqlcontext.sql("drop table if exists sp_class.tmp_contact_phone_df")

    contact_userinfo_df.saveAsTable("sp_class.tmp_contact_phone_df")

    ssqlcontext.dropTempTable("sp_class_tmp_contact_phone_df")
  }

  def generateInvoice(ssqlcontext:HiveContext,sc:SparkContext): Unit ={

    //sp_class.tmp_user_city_list保证了user_id,city是唯一对，即 保证了sp_class.df_tmp_ordercity_rate一个用户的city唯一性

    ssqlcontext.sql("drop table if exists sp_class.df_tmp_ordercity_rate")

    val sql1=
      """
        |    create table sp_class.df_tmp_ordercity_rate as
        |     select a.user_id, a.city, a.cnt / allcnt as rate
        |       from sp_class.tmp_user_city_list a
        |       join (select user_id, sum(cnt) as allcnt
        |               from sp_class.tmp_user_city_list
        |              group by user_id) b
        |         on a.user_id = b.user_id
      """.stripMargin

    ssqlcontext.sql(sql1)



    ssqlcontext.sql("drop table if exists sp_class.df_tmp_invoice_address_rate ")


    val sql2=
      """
        |create table sp_class.df_tmp_invoice_address_rate as
        |select a.user_id, a.invoice_address, a.cnt / b.allcnt as rate
        | from (select user_id, invoice_address, count(1) as cnt
        |         from sp_class_tmp_user_orders
        |        where invoice_address != 'NULL'
        |        group by user_id, invoice_address) a
        | join (select user_id, count(1) as allcnt
        |         from sp_class_tmp_user_orders
        |        where invoice_address != 'NULL'
        |        group by user_id) b
        |   on a.user_id = b.user_id
      """.stripMargin

      ssqlcontext.sql(sql2)


    val sql3=
      """
        | select t.user_id, s.phone_city, t.invoice_address, t.rate
        |   from (select user_id, phone_city
        |           from s_user_info
        |          where phone_city != 'NULL') s
        |   join sp_class.df_tmp_invoice_address_rate t
        |     on s.user_id = t.user_id
      """.stripMargin


      val s_phone_invoice_df = ssqlcontext.sql(sql3)


      val s_phone_arr = ArrayBuffer[(String, String)]()

      val s_phone_retdf =  s_phone_invoice_df.map( r=>{
          val user_id = r.getAs[String]("user_id")
          val phone_city = r.getAs[String]("phone_city")
          val invoice_address = r.getAs[String]("invoice_address")
          val rate =  r.getAs[Double]("rate")

          if(!phone_city.isEmpty && invoice_address.contains(phone_city)){
              s_phone_arr.append( (user_id, invoice_address) )
              Row(user_id, phone_city, rate)
          }else{
              Row()
          }
    })


    val s_phone_invoice =  s_phone_arr.map(r=>{
      Row(r._1, r._2)
    })


    val s_phone_schema = StructType(
        StructField("user_id", StringType)::
        StructField("invoice_address", StringType)
        ::Nil)



    ssqlcontext.sql("drop table if exists sp_class.df_tmp_already_cal_add ")


    ssqlcontext.createDataFrame(  sc.makeRDD(s_phone_invoice.toList), s_phone_schema)
                                                 .saveAsTable("sp_class.df_tmp_already_cal_add")


    val sql4=
      """
        |select u.*, t.invoice_address, t.rate as invoice_rate
        |  from sp_class.df_tmp_ordercity_rate u
        |  join (select a.user_id, a.invoice_address, a.rate
        |          from sp_class.df_tmp_invoice_address_rate a
        |          join sp_class.df_tmp_already_cal_add b
        |            on a.user_id = b.user_id
        |         where a.invoice_address != b.invoice_address) t
        |    on u.user_id = t.user_id
      """.stripMargin


    val inv_df = ssqlcontext.sql(sql4)


    var inv_retdf  = inv_df.map( r=> {

        val user_id = r.getAs[String]("user_id")

        val city = r.getAs[String]("city")

        val invoice_address = r.getAs[String]("invoice_address")
        val invoice_rate =  r.getAs[Double]("invoice_rate")
        if(invoice_address.contains(city)  ){
           Row(user_id, city, invoice_rate)
        }else{
           Row()
        }
    })

    inv_retdf = inv_retdf ++ s_phone_retdf
    inv_retdf  = inv_retdf.filter( x => x!=Row() )


    val invoice_schema = StructType(
      StructField("user_id", StringType) ::
        StructField("city", StringType ) ::
        StructField("rate", DoubleType)
        :: Nil)

    var df_invoice = ssqlcontext.createDataFrame(inv_retdf, invoice_schema)

    inv_retdf = df_invoice.map( r=> {
         val key=(r.getAs[String]("user_id"), r.getAs[String]("city"))
         (key, r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (X) => {
          Row(X._1._1, X._1._2, X._2)
    })

    df_invoice = ssqlcontext.createDataFrame(inv_retdf, invoice_schema)

    ssqlcontext.sql("drop table if exists sp_class.df_tmp_invoice")

    df_invoice.saveAsTable("sp_class.df_tmp_invoice")

    ssqlcontext.sql("drop table if exists sp_class.df_tmp_already_cal_add ")
  }




}
