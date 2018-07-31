package com.hbgj.test

import java.net.URI
import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

object Testa2 {
  def main(args: Array[String]): Unit = {
    val conf=new  SparkConf()

    conf.setMaster("local[8]").setAppName("test-2")

    val sc=new SparkContext(conf)

    val hsc=new HiveContext(sc)

    hsc.sql("drop table if exists sp_class.df_tmp_latlnt_phoneid")

    val fs=FileSystem.get(new URI("hdfs://hd3:9000/"),new Configuration())
    val path=new Path("hdfs://hd3:9000/user/hive/warehouse/sp_class.db/df_tmp_latlnt_phoneid")
    if(fs.exists(path))fs.delete(path)

    val rrd=sc.textFile("D:\\zxy\\tempdir\\rdd" ).map(line=>{

        val ln=line.substring(1,line.length-1).split(",")
      var time: Timestamp =null
      try {
            time= Timestamp.valueOf(ln(2))
      }catch {
        case e:Exception => {
                println(" Timestamp convert exception : "+ln(2))
                time= Timestamp.valueOf("1976-07-01 05:56:20.0")
        }
      }

      var num:Int = -1;
      try {
        num= ln(3).toInt
      }catch {
        case e:Exception => {
          println(" Int convert exception => "+ln(3))
        }
      }



        Row(ln(0),ln(1),time ,num)
    })

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



    loc_ret_df.saveAsTable("sp_class.df_tmp_latlnt_phoneid")

    hsc.dropTempTable("sp_class_loc_phoneid")




    sc.stop()


  }
}
