package com.huoli.statictag.model


import java.sql.{DriverManager, ResultSet}
import java.util.{Date, Properties}

import com.huoli.statictag.utils.Config
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 找出用户的居住城市 ------------> 常驻城市
  *
  * 查询一年的数据 , 默认 一周调度一次
  *
  *
  * 手机归属地                 权重           0.1
  * 出行行程                   权重           0.6
  * 活跃城市(经纬度表)          权重           0.2
  * 身份证地址                  权重           0.1
  *
  * create date 2018-08-02
  * auth zengxy@133.cn
  *
  **/

object ResidentCityNew {

  //手机归属地                 权重           0.1
  private val  PHONE_WEIGHT:Double=0.2
  //出行行程                   权重           0.6
  private val  TRAVEL_WEIGHT:Double=0.25
  //活跃城市(经纬度表)          权重           0.2
  private val  ACTIVE_CITY_WEIGHT:Double=0.5
  //身份证地址                  权重           0.1
  private val  CARE_ID_WEIGHT:Double=0.05

  private  var hsc:HiveContext=null

  private  var   S_AIRLINE_ORDER_DATE="2017-07-01"

  private  var   AREABY_LATLNT_EVENT_DATE="2017-07-01"

  private var     STATICS_DATE="201807"


  private  val logger:Logger=Logger.getLogger(this.getClass)

  private  val  dateFormat:FastDateFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    *手机归属地                 权重           0.1
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


  /**
    * 手机归属和出行权重可以合并在一起查询
    */
  def   contactPhoneWeight(): Unit ={
    val phone_sql1=
      s"""
         |select  o.user_id,
         |        o.contact_phone,
         |        d.dep_city_name,
         |        d.arr_city_name,
         |        d.passenger_uid
         |   from s_airline_order_detail  d,
         |    s_airline_order o
         |    where o.dt >= '$S_AIRLINE_ORDER_DATE'  and o.dt=d.dt
         |     and d.order_id = o.order_id  and
         |        o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75')
      """.stripMargin

    val phone_travel_df1:DataFrame = hsc.sql(phone_sql1)
    phone_travel_df1.registerTempTable("temp_phone_travel_table1")

    val phone_sql3="""
                  select t1.user_id ,t1.contact_phone,t1.dep_city_name,t1.arr_city_name  from temp_phone_travel_table1 t1,s_user_info u where u.user_id = t1.user_id and
                 (u.passport = t1.passenger_uid OR u.id_card = t1.passenger_uid)

                  """
    val phone_travel_df3=hsc.sql(phone_sql3)


    phone_travel_df3.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.tmp_weight_city_phone_travel")

    hsc.dropTempTable("temp_phone_travel_table1")
    /**
      *
      * join s_user_info u
      * on (u.user_id = o.user_id and
      * (u.passport = d.passenger_uid OR u.id_card = d.passenger_uid))
      */

    /**
      *default.phone_location  手机归属地查询
      * 手机归属地                 权重           0.1
      *
      * 手机号 的来源分为两个部分
      *
      * 1. 订单详情表中的联系电话
      *
      *  由于订单是一段时间的订单，故不能概括所有用户
      *
      *  因而增加  用户信息表中的手机号
      *
      * 2. 用户信息表中 phone_number 用于补充
      *
      * select  phone_number  from s_user_info limit 1;
      *
      *
      */

    val phone_sql2=
      """
        |select t.user_id, t.phone_prefix, count(1) cnt
        |  from (select user_id,
        |               substr(phone_number, 0, 7) phone_prefix from s_user_info union all
        |                select user_id, substr(contact_phone, 0, 7) phone_prefix
        |                  from tmp.tmp_weight_city_phone_travel
        |        ) t
        | group by t.user_id, t.phone_prefix
        |
        """.stripMargin

    val phone_all_temp_df:DataFrame = hsc.sql(phone_sql2)

    phone_all_temp_df.registerTempTable("weight_city_phone_temp")

    val phone_sql4=
      s"""
         |select u.user_id, pl.city, sum(u.rate) * $PHONE_WEIGHT rate
         |  from (select t.user_id,
         |               t.phone_prefix,
         |               (t.cnt / sum(t.cnt) over(partition by t.user_id)) rate
         |          from weight_city_phone_temp t) u
         |  join default.phone_location pl
         |    on u.phone_prefix = pl.phone_number
         | group by u.user_id, pl.city
      """.stripMargin

    val phone_df:DataFrame = hsc.sql(phone_sql4)

    phone_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.tmp_weight_city_phone")

    hsc.dropTempTable("weight_city_phone_temp")

  }
  /**
    * 出行   权重
    * 出行行程                   权重           0.6
    * phone_travel_df.registerTempTable("temp_phone_travel_table")
    */
  def  travelWeight(): Unit ={

    val travel_sql=
      s"""
         |select t2.user_id,
         |       t2.city,
         |       (t2.cnum / (sum(t2.cnum) over(partition by t2.user_id))) * $TRAVEL_WEIGHT rate
         |  from (select t1.user_id, t1.city, count(1) cnum
         |          from (select user_id, dep_city_name city
         |                  from tmp.tmp_weight_city_phone_travel
         |                union all
         |                select user_id, arr_city_name city
         |                  from tmp.tmp_weight_city_phone_travel  ) t1
         |         group by t1.user_id, t1.city) t2
         |
      """.stripMargin

    val travel_df:DataFrame = hsc.sql(travel_sql)

    travel_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.tmp_weight_city_travel")

  }


  /**
    *
    * 活跃城市(经纬度表)          权重           0.2
    *
    */

  def  activeCityWeightByLatlnt(): Unit ={
    val active_sql=
      s"""
         |select t2.user_id,
         |       t2.city,
         |       (t2.cnum / ( sum(t2.cnum) over(partition by t2.user_id))) * $ACTIVE_CITY_WEIGHT rate
         |  from (select t1.phoneid user_id, t1.city, count(1) cnum
         |                  from sp_class.areaby_latlnt t1
         |                 where  t1.event_date >= '$AREABY_LATLNT_EVENT_DATE'  and   t1.phoneid != 'NULL'
         |         group by t1.phoneid, t1.city ) t2
       """.stripMargin

    val active_city_df:DataFrame = hsc.sql(active_sql)

    active_city_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.tmp_weight_city_active")

  }

  /**
    *  身份证地址                  权重           0.1
    *  全量数据
    *
    */
  def  cardIdWeight(): Unit ={

    val card_sql=
      s"""
         |select t1.user_id, t1.born_city city, $CARE_ID_WEIGHT rate
         |  from s_user_info t1
      """.stripMargin

    val id_card_df:DataFrame = hsc.sql(card_sql)

    id_card_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("tmp.tmp_weight_city_id_card")
  }

  def combineToResult(): Unit = {

    val tableName=s"tmp.tmp_resident_city_$STATICS_DATE"
    hsc.sql(s"drop table if exists  $tableName ")

    val combine_sql_1=
      s"""
         |select t2.user_id,
         |       t2.city,
         |       row_number() over(partition by t2.user_id order by t2.all_rate desc) rk
         |  from (select t1.user_id, t1.city, ( case when  t1.city='未知'  then 0 else sum(t1.rate)  end ) all_rate
         |          from (select p.user_id, ( case when ( length(p.city)=0 or p.city is null )  then '未知'  else p.city  end ) city, p.rate
         |                  from tmp.tmp_weight_city_phone   p
         |                union all
         |                select t.user_id, ( case when ( length(t.city)=0 or t.city is null )  then '未知'  else t.city  end ) city, t.rate
         |                  from tmp.tmp_weight_city_travel   t
         |                union all
         |                select a.user_id, ( case when ( length(a.city)=0 or a.city is null )  then '未知'  else a.city  end ) city, a.rate
         |                  from tmp.tmp_weight_city_active   a
         |                union all
         |                select c.user_id, ( case when ( length(c.city)=0 or c.city is null )  then '未知'  else c.city  end ) city, c.rate
         |                    from tmp.tmp_weight_city_id_card c) t1
         |         group by t1.user_id, t1.city) t2
      """.stripMargin

    //) t3 where t3.rk<=4 group by t3.user_id

    val combine_df=hsc.sql(combine_sql_1)

    combine_df.registerTempTable("tmp_combine_resident_city")


    val combine_sql_2=
      s"""
        |create table  $tableName  as
        |select p1.user_id,p2.active_city,p1.resident_city   from
        | (select t1.user_id,t1.city resident_city from tmp_combine_resident_city  t1 where t1.rk=1 ) p1,
        | (select t2.user_id,concat_ws(',',collect_set(t2.city)) active_city from  tmp_combine_resident_city t2 where t2.rk<=4 group by t2.user_id)p2
        |  where p1.user_id=p2.user_id
      """.stripMargin

    hsc.sql(combine_sql_2)


    //combine_df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(s"tmp.tmp_resident_city_$STATICS_DATE")
/*    hsc.setConf("spark.sql.shuffle.partitions","500")
    val prop =new Properties()
    val url= Config("etl_db.url")
    val username= Config("etl_db.username")
    val password=Config("etl_db.password")
    prop.setProperty("user",username)
    prop.setProperty("password",password)
    combine_df.write.mode(SaveMode.Overwrite).jdbc(url,mysqlTbName,prop)*/
  }

  def main(args: Array[String]): Unit = {
/*
    private  var   S_AIRLINE_ORDER_DATE="2017-07-01"

    private  var   AREABY_LATLNT_EVENT_DATE="2017-07-01"

    private var     STATICS_DATE="201807"*/


    logger.info(" begin to analysis user's data : "+dateFormat.format(new Date()))

    if(args==null || args.length<3){
      logger.info("parmas's number must be 3 ")
      return
    }

    S_AIRLINE_ORDER_DATE=args(0)
    AREABY_LATLNT_EVENT_DATE=args(1)
    STATICS_DATE=args(2)

    val conf=new  SparkConf()
    val sc=new SparkContext(conf)
    hsc=new HiveContext(sc)
    logger.info(" create  HiveContext to operation hive use sql  : "+dateFormat.format(new Date()))
    logger.info("  开始分析用户的手机归属地 : "+dateFormat.format(new Date()))
    contactPhoneWeight()
    logger.info("  开始分析用户的行程  : "+dateFormat.format(new Date()))
    travelWeight()
    logger.info("  开始分析用户的活跃城市  : "+dateFormat.format(new Date()))
    activeCityWeightByLatlnt()
    logger.info("  开始分析用户的户籍城市  : "+dateFormat.format(new Date()))
    cardIdWeight()
    logger.info(s"  合并以上分析用户的数据,将分析结果入库  tmp.tmp_resident_city_$STATICS_DATE : "+dateFormat.format(new Date()))
    combineToResult()
    logger.info(" 入库完毕，结束分析 :"+dateFormat.format(new Date()))
    sc.stop()

  }

  def execute(sql:String)={
    // Change to Your Database Config
    val conn_str = Config("etl_db.url") + "&user=" + Config("etl_db.username") + "&password=" + Config("etl_db.password")
    // Load the driver
    Class.forName("com.mysql.jdbc.Driver")
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try{
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      statement.execute(sql)
    }finally {
      conn.close
    }
  }
  def exportHivtToMysql(hiveTable:String ,tbname : String) = {




    execute(s"truncate table ${tbname}")

    import scala.sys.process._
    val url= Config("etl_db.url")
    val username= Config("etl_db.username")
    val password=Config("etl_db.password")
    val sqoop_cmd=s"""
       |sqoop export  -m 15 --connect $url
       |--username $username
       |--password  $password
       |--table  $tbname
       |-outdir /home/bigdata/data_job_workspace/sqoop_outdir
       |--export-dir /hive/warehouse/sp_class.db/$hiveTable
       |--fields-terminated-by '\0001'
       |--input-null-string 'NULL'
       |--input-null-non-string 'NULL'
     """.stripMargin

    //执行sqoop命令
     sqoop_cmd.!

  }
}
