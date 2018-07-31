package com.hbgj.test

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkSqlQuery {
  def main(args: Array[String]): Unit = {
    val conf=new  SparkConf()
    //conf.setMaster("local[50]").setAppName("test-query")
    val sc=new SparkContext(conf)
    val hsc=new HiveContext(sc)

    val sql=
      """
        |select *   from phone_location limit 100
      """.stripMargin
    val df=hsc.sql(sql)

    val dt="201808"

/*    hsc.setConf("hive.exec.dynamic.partition","true")

    hsc.setConf("hive.exec.dynamic.partition.mode","nonstrict")*/




    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable("temp_user"+dt)


    //df.saveAsTable("temp_stu2")

    sc.stop()

  }
}
