package com.hbgj.test
package com.huoli.statictag.model

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.sys.process._
/**
  * Created by lxb on 2017/6/30.
  * ResidentCity
  *
  *  phoneid                city rate rn
  * /KqITDeVzTpRKugX208cQ==	青岛	0.5	1
  * 1Lo7C0GU67pRKugX208cQ==	无锡	0.5	1
  * MK 7gFl2M=	北京	0.5	1
  * /KLe0fOapo=	北京	0.5	1
  * 0GyWivTybjpRKugX208cQ==	天津	0.5	1
  * 1UQ i6mQ4Q=	深圳	0.5	1
  * 2SXQ7QZHPo=	昭通	0.5	1
  * 39GFeuKIyg=	南昌	0.5	1
  * 3Vi1k/TdUo=	北京	0.5	1
  * 46Blfz0nefpRKugX208cQ==	上海	0.5	1
  * 4AO2XqirYfpRKugX208cQ==	北京	0.25	1
  * 4YNIkFgigc=	太原	0.5	1
  * 4oekJeJYFrpRKugX208cQ==	天津	0.5	1
  * 5kJWIHDsmM=	沈阳	0.5	1
  * 7wSgEjeRCTpRKugX208cQ==	眉山	0.5	1
  * DRCT8rJlFzpRKugX208cQ==	北京	0.5	1
  * EDSrzoTGQjpRKugX208cQ==	无锡	0.5	1
  * FMoBPNxmtvpRKugX208cQ==	北京	0.5	1
  * GCmnwoiz1bpRKugX208cQ==	苏州	0.25	1
  * GUOf6jU4ULpRKugX208cQ==	北京	0.375	1
  * GgzN3MBHADpRKugX208cQ==	西安	0.5	1
  * PYhxRyVS6PpRKugX208cQ==	成都	0.5	1
  * Pl261MyI3fpRKugX208cQ==	青岛	0.2	1
  */
case class InvoiceRecord(invoice_address: String, rate: Double, dateLst: Array[String]) {}


case class FlyTuple(city:String, r:Double) {}

object ResidentCity {

  private var ssqlcontext:SQLContext =null ;
  private var sc:SparkContext =null;

  private def getSqlContext(): SQLContext ={
    val conf = new SparkConf().setAppName("ResidentCity").setMaster("yarn-client")
    sc = new SparkContext(conf)

    /**
      *
      * spark.scheduler.mode	FIFO  FAIR
      * 决定 单个Spark应用(同一个sparkcontext) 内部调度的时候使用FIFO模式还是Fair模式
      * 默认是FIFO，即谁先提交谁先执行，而FAIR支持在调度池中再进行分组，可以有不同的权重，根据权重、资源等来决定谁先执行
      *
      *多个Spark应用间的调度策略由Yarn自己的策略配置文件所决定
      */
    sc.setLocalProperty("spark.scheduler.pool", "myfair")

    val sqlContext = new HiveContext(sc)
    sqlContext
  }

  def getSingleContext():SQLContext = {
    if(ssqlcontext==null){
      ssqlcontext = getSqlContext()
    }
    ssqlcontext
  }

  def getTopCityFlys(cityMap :mutable.HashMap[String,Integer], k:Int ):List[(String, Integer)] = {
    var orderLst = cityMap.toList.sortBy(_._2)(KeyOrdering);
    orderLst.take(k)
  }

  implicit val KeyOrdering = new Ordering[Integer] {
    override def compare(x: Integer, y: Integer): Int = {
      y-x
    }
  }

  def generateLatlnt(): Unit ={
    ssqlcontext = getSingleContext()
    System.setProperty("HADOOP_USER_NAME", "hive")

    var detailloc = ssqlcontext.sql("select * from " +
      "(select phoneid,city,event_time,event_date from sp_class.areaby_latlnt where event_date>='2017-02-22'" +
      " and phoneid!='NULL' order by phoneid,event_time) s" +
      " left join (select o.user_id, collect_list(concat(d.dep_time,'|',d.dep_city_name,',',d.arr_city_name)) " +
      "as ticketslist  from s_airline_order_detail d join s_airline_order o on (d.order_id=o.order_id  " +
      " and o.orderstatue IN ('5','52','53','71','73','74','75') and d.dep_time>='2017-02-20 00:00:00') " +
      " join  s_user_info u  on (u.user_id=o.user_id) where  u.id_card=d.passenger_uid " +
      "or u.passport=d.passenger_uid  group by o.user_id ) u " +
      " on s.phoneid=u.user_id ").rdd

    //    detailloc.saveAsTextFile("/user/root/tempfile")
    var pre_city:String = null;
    var rrd  = detailloc.map(f = r => {
      val phoneid = r.getAs[String]("phoneid")
      val city = r.getAs[String]("city")
      val etime: Timestamp = r.getAs[Timestamp]("event_time")

      val ticketslist = r.getAs[Seq[String]]("ticketslist");
      if (ticketslist != null) {
        var tmp = ticketslist.map(r => (r.split('|').head, r.split('|').last.split(',').mkString(",")) )
        //println(tmp.mkString(","))
        //var ticketcity = ticketslist.map(r => r.split('|').last.split(',') )
        var flag=0;
        //可以再进行精确一步，后期可优化
        ticketslist.foreach(r => {
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val t = format.parse(r.split('|').head);
          val order_citys:Array[String] = r.split('|').last.split(',')
          var t_0 = format.parse(r.split('|').head);
          t_0.setTime(t.getTime+3600000);
          if (etime.before(t_0) && city.equals(order_citys.head)) {
            flag = 1;
          }else if(etime.after(t) && city.equals(order_citys.last)){
            flag = 1;
          }

        })
        if(pre_city==null || !pre_city.equals(city)){
          pre_city = city;
          //(phoneid,city,etime, flag, tmp.mkString(";"))
          Row(phoneid,city,etime, flag)
        }else{
          Row()
        }

      } else {
        if(pre_city==null || !pre_city.equals(city)) {
          pre_city = city;
          //(phoneid, city, etime, false)
          Row(phoneid, city, etime, 0)
        }else{
          Row()
        }
      }

    })
    rrd  = rrd.filter( x => x!=Row() )
    //rrd.saveAsTextFile("/user/root/file"+System.currentTimeMillis())

    val schema = StructType(
      StructField("phoneid", StringType) ::
        StructField("city", StringType)::
        StructField("etime", TimestampType)::
        StructField("flag", IntegerType)
        :: Nil)
    var loc_df=ssqlcontext createDataFrame(rrd, schema);
    loc_df.registerTempTable("sp_class_loc_phoneid");
    var loc_ret_df = ssqlcontext.sql("select phoneid as user_id,city,sum(rate)*0.5 as rate from ( select a.phoneid,city,flag,1/allcnt as rate from sp_class_loc_phoneid a join (select phoneid,count(1) as allcnt from sp_class_loc_phoneid group by phoneid ) t on a.phoneid=t.phoneid  where flag=0 ) u group by phoneid,city")
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_latlnt_phoneid")
    loc_ret_df.saveAsTable("sp_class.df_tmp_latlnt_phoneid")
    ssqlcontext.dropTempTable("sp_class_loc_phoneid")
  }

  def generateAllLatlnt(): Unit ={
    ssqlcontext = getSingleContext()
    System.setProperty("HADOOP_USER_NAME", "hive")
    ssqlcontext.sql("drop table if exists sp_class.tmp_areaby_latlnt_resident ")
    ssqlcontext.sql("create table sp_class.tmp_areaby_latlnt_resident as select phoneid,city,event_date from sp_class.areaby_latlnt where event_date>='2017-02-22' and phoneid!='NULL' group by phoneid,city,event_date")

    var loc_ret_df = ssqlcontext.sql("select a.phoneid as user_id,a.city,a.cnt/t.allcnt*0.5 as rate from ( select phoneid,city,count(1) as cnt from sp_class.tmp_areaby_latlnt_resident where city is not null and city!='' group by phoneid,city ) a join (select phoneid,count(1) as allcnt from sp_class.tmp_areaby_latlnt_resident group by phoneid ) t on a.phoneid=t.phoneid ")
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_latlnt_phoneid")
    loc_ret_df.saveAsTable("sp_class.df_tmp_latlnt_phoneid")
    ssqlcontext.sql("drop table if exists sp_class.tmp_areaby_latlnt_resident ")
  }

  def generateContactPhone():Unit ={

    val df:DataFrame = ssqlcontext.sql("select u.user_id,d.dep_city_name,d.arr_city_name,d.dep_time,o.order_id, o.invoice_address,o.contact_phone ,o.dt  from s_airline_order_detail d join s_airline_order o on (d.order_id=o.order_id  and o.orderstatue IN ('5','52','53','71','73','74','75') ) join  s_user_info u on (u.user_id=o.user_id and ( u.passport = d.passenger_uid OR u.id_card = d.passenger_uid ) )  ")
    df.registerTempTable("sp_class_tmp_user_orders")
    //缺少电话联系方式的人员，加入订单内的联系方式
    val contact_phone_df = ssqlcontext.sql("select user_id,l.city,u.phone_prefix,u.rate  from (select a.user_id,substr(a.contact_phone,0,7) as phone_prefix, a.cnt/t.allcnt as rate from (select user_id, contact_phone, count(1) cnt from sp_class_tmp_user_orders group by user_id,contact_phone) a join (select user_id,count(1) as allcnt from  sp_class_tmp_user_orders group by user_id ) t  on a.user_id=t.user_id ) u join default.phone_location l on u.phone_prefix=l.phone_number ")
    contact_phone_df.registerTempTable("sp_class_tmp_contact_phone_df");
    val contact_userinfo_df = ssqlcontext.sql(" select a.user_id,a.city,a.rate from sp_class_tmp_contact_phone_df a join s_user_info s on a.user_id=s.user_id where s.phone_city is null")
    ssqlcontext.sql("drop table if exists sp_class.tmp_contact_phone_df")
    contact_userinfo_df.saveAsTable("sp_class.tmp_contact_phone_df");  //for test
    ssqlcontext.dropTempTable("sp_class_tmp_contact_phone_df")
  }

  def generateInvoice(): Unit ={
    ssqlcontext = getSingleContext()
    //sp_class.tmp_user_city_list保证了user_id,city是唯一对，即 保证了sp_class.df_tmp_ordercity_rate一个用户的city唯一性
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_ordercity_rate")
    ssqlcontext.sql("create table sp_class.df_tmp_ordercity_rate as select a.user_id, a.city, a.cnt/allcnt as rate from sp_class.tmp_user_city_list a join (select user_id, sum(cnt) as allcnt from sp_class.tmp_user_city_list group by user_id ) b on a.user_id=b.user_id ")
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_invoice_address_rate ")
    ssqlcontext.sql("create table sp_class.df_tmp_invoice_address_rate as select a.user_id,a.invoice_address,a.cnt/b.allcnt as rate from (select user_id, invoice_address, count(1) as cnt from sp_class_tmp_user_orders where invoice_address!='NULL' group by user_id,invoice_address) a join (select user_id, count(1) as allcnt from sp_class_tmp_user_orders where invoice_address!='NULL' group by user_id ) b on a.user_id=b.user_id ")

    var s_phone_invoice_df = ssqlcontext.sql("select t.user_id,s.phone_city,t.invoice_address,t.rate  from (select user_id,phone_city from s_user_info where phone_city!='NULL') s join sp_class.df_tmp_invoice_address_rate t  on s.user_id=t.user_id ")
    var s_phone_arr = mutable.ArrayBuffer[Tuple2[String,String]]()
    var s_phone_retdf =  s_phone_invoice_df.map( r=>{
      var user_id = r.getAs[String]("user_id");
      var phone_city = r.getAs[String]("phone_city");
      var invoice_address = r.getAs[String]("invoice_address");
      var rate =  r.getAs[Double]("rate")
      if(!phone_city.isEmpty && invoice_address.contains(phone_city)){

        s_phone_arr.append( Tuple2(user_id, invoice_address) );

        Row(user_id, phone_city, rate)
      }else{
        Row()
      }
    })
    var s_phone_invoice =  s_phone_arr.map(r=>{
      Row(r._1, r._2)
    });
    val s_phone_schema = StructType(
      StructField("user_id", StringType) ::
        StructField("invoice_address", StringType ) :: Nil );
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_already_cal_add ")
    ssqlcontext.createDataFrame(sc.makeRDD(s_phone_invoice.toList), s_phone_schema).saveAsTable("sp_class.df_tmp_already_cal_add");

    var inv_df = ssqlcontext.sql(" select  u.*, t.invoice_address, t.rate as invoice_rate from sp_class.df_tmp_ordercity_rate u join (select a.user_id,a.invoice_address,a.rate from sp_class.df_tmp_invoice_address_rate a join sp_class.df_tmp_already_cal_add b on a.user_id=b.user_id where a.invoice_address!=b.invoice_address)t on u.user_id=t.user_id ")
    var inv_retdf  = inv_df.map( r=> {
      var user_id = r.getAs[String]("user_id");
      var city = r.getAs[String]("city");
      var invoice_address = r.getAs[String]("invoice_address");
      var invoice_rate =  r.getAs[Double]("invoice_rate")
      if(invoice_address.contains(city)  ){
        Row(user_id, city, invoice_rate)
      }else{
        Row()
      }
    })
    inv_retdf = inv_retdf ++ s_phone_retdf;
    inv_retdf  = inv_retdf.filter( x => x!=Row() )
    val invoice_schema = StructType(
      StructField("user_id", StringType) ::
        StructField("city", StringType ) ::
        StructField("rate", DoubleType)
        :: Nil);
    var df_invoice = ssqlcontext.createDataFrame(inv_retdf, invoice_schema);
    inv_retdf = df_invoice.map( r=> {
      (Tuple2(r.getAs[String]("user_id"), r.getAs[String]("city")), r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (X) => {
      Row(X._1._1, X._1._2, X._2)
    })
    df_invoice = ssqlcontext.createDataFrame(inv_retdf, invoice_schema);

    ssqlcontext.sql("drop table if exists sp_class.df_tmp_invoice");
    df_invoice.saveAsTable("sp_class.df_tmp_invoice");
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_already_cal_add ")
  }

  def main(args: Array[String]) {
    ssqlcontext = getSingleContext()
    System.setProperty("HADOOP_USER_NAME", "hive")

    generateAllLatlnt()
    generateContactPhone()

    val df:DataFrame = ssqlcontext.sql("select u.user_id,d.dep_city_name,d.arr_city_name,d.dep_time,o.order_id, o.invoice_address,o.contact_phone ,o.dt  from s_airline_order_detail d join s_airline_order o on (d.order_id=o.order_id  and o.orderstatue IN ('5','52','53','71','73','74','75') ) join  s_user_info u on (u.user_id=o.user_id and ( u.passport = d.passenger_uid OR u.id_card = d.passenger_uid ) )  ")
    df.registerTempTable("sp_class_tmp_user_orders")

    ssqlcontext.sql(" drop table if exists sp_class.tmp_user_city_list")
    ssqlcontext.sql(" create table if not exists sp_class.tmp_user_city_list (user_id string, city string, cnt int) ")
    ssqlcontext.sql("insert into sp_class.tmp_user_city_list  select user_id, city , count(1) as cnt from (select user_id, explode(split(concat_ws(',',dep_city_name,arr_city_name),',')) as city  from  sp_class_tmp_user_orders ) t group by user_id,city ")
    var rdd_topcity = ssqlcontext.sql("select a.user_id,a.city, a.cnt/allcnt as rate from sp_class.tmp_user_city_list a left join (select user_id, sum(cnt) as allcnt from sp_class.tmp_user_city_list group by user_id ) b on a.user_id=b.user_id  ")


    var phoneCityMap = mutable.HashMap[String, String]()
    var rdd_phonecity = ssqlcontext.table("s_user_info").where("phone_city!='NULL'").select("user_id","phone_city").map( r=>{
      var user_id = r.getAs[String]("user_id");
      var phone_city = r.getAs[String]("phone_city");
      phoneCityMap.put(user_id,phone_city)
      Row(user_id, phone_city)
    })
    ssqlcontext.sql("select COALESCE(s.user_id,t.user_id) as user_id, case when s.phone_city is null then '' else s.phone_city end as phone_city,case when t.city is null then '' else t.city end as city,case when t.rate is null then 0 else t.rate end as rate from (select user_id,phone_city from s_user_info where phone_city!='NULL' ) s full outer join (select a.user_id,a.city, a.cnt/allcnt as rate from sp_class.tmp_user_city_list a join (select user_id, sum(cnt) as allcnt from sp_class.tmp_user_city_list group by user_id ) b on a.user_id=b.user_id ) t on s.user_id=t.user_id ").registerTempTable("sp_class_tmp_user_order_list")
    //for test
    ssqlcontext.sql("drop table if exists sp_class.df_tmp_sp_class_tmp_user_order_list");
    ssqlcontext.sql("create table sp_class.df_tmp_sp_class_tmp_user_order_list  as select * from sp_class_tmp_user_order_list")

    generateInvoice();
    //var invoice_df  = ssqlcontext.sql("select a.user_id,a.invoice_address,a.cnt/b.allcnt as rate from (select user_id, invoice_address, count(1) as cnt from sp_class_tmp_user_orders where invoice_address!='NULL' group by user_id,invoice_address) a join (select user_id, count(1) as allcnt from sp_class_tmp_user_orders where invoice_address!='NULL' group by user_id ) b on a.user_id=b.user_id ")
    //var invoiceMap = mutable.HashMap[String,ArrayBuffer[InvoiceRecord]]()
    val invoice_schema = StructType(
      StructField("user_id", StringType) ::
        StructField("city", StringType ) ::
        StructField("rate", DoubleType)
        :: Nil);

    var df_sp = ssqlcontext.sql("select user_id, city, rate from sp_class_tmp_user_order_list where city!='' union all select user_id, phone_city as city, 1 as rate from s_user_info where phone_city!='NULL' ")
    var loc_ret_df = ssqlcontext.table("sp_class.df_tmp_latlnt_phoneid")
    var df_invoice = ssqlcontext.sql("select user_id, city, rate*1.2 as rate from sp_class.df_tmp_invoice")
    var df_rret = df_sp.unionAll(df_invoice).unionAll(loc_ret_df)
    var  rdd_tmp = df_rret.map(r => {
      (Tuple2(r.getAs[String]("user_id"), r.getAs[String]("city")), r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (X) => {
      Row(X._1._1, X._1._2, X._2)
    })
    val  df_tmp = ssqlcontext.createDataFrame(rdd_tmp, invoice_schema);
    var contact_userinfo_df = ssqlcontext.table("sp_class.tmp_contact_phone_df")
    df_rret = df_tmp.unionAll(contact_userinfo_df)
    ssqlcontext.dropTempTable("sp_class_tmp_user_order_list");
    ssqlcontext.sql("drop table if exists sp_class.resident_city");
    val rdd_ret = df_rret.map(r => {
      (Tuple2(r.getAs[String]("user_id"), r.getAs[String]("city")), r.getAs[Double]("rate"))
    }).reduceByKey((x,y)=> x+y).map( (X) => {
      Row(X._1._1, X._1._2, X._2)
    })

    val df_ret = ssqlcontext.createDataFrame(rdd_ret, invoice_schema)
    df_ret.registerTempTable("sp_class_tmp_city_rate")
    ssqlcontext.sql("drop table if exists sp_class.resident_city_001")
    ssqlcontext.sql("create table sp_class.resident_city_001 as select user_id,city,rate, row_number() over (partition by user_id order by rate desc ) rn from sp_class_tmp_city_rate ")
    ssqlcontext.sql("create table sp_class.resident_city as select user_id,city,rate,  rn from sp_class.resident_city_001 where rn=1 ")


  }




}


