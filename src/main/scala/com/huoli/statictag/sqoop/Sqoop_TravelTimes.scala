package com.huoli.statictag.sqoop
import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Logger

/**
  *
  * 将用户出行类型
  * 小于5 －低频     5-15 普通      15-25 高频       25＋ 超高频
  * 从hive库导入mysql数据库
  *
  */
object Sqoop_TravelTimes {
  private val logger:Logger=Logger.getLogger(this.getClass)
  private  val  dateFormat:FastDateFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]) {
    logger.info(" begin to export hive data to mysql  : "+dateFormat.format(new Date()))
    if(args==null || args.length<2){
      logger.info("parmas's number must be 2 ")
      return
    }

    val mysqlTableName=args(0)
    val hiveTableName=args(1)
    val hiveTableInfo=parseTableInfomation(hiveTableName)
    //导入数据
    SqoopUtil.insertTagTabTemp(mysqlTableName,hiveTableName=hiveTableInfo._2,hivedb=hiveTableInfo._1)
    logger.info(" begin to rename  mysql   table  : "+dateFormat.format(new Date()))
    //rename mysql table
    SqoopUtil.renameTables(mysqlTableName)
    logger.info(" programe is end  : "+dateFormat.format(new Date()))
  }

  def parseTableInfomation(hiveTableName:String): (String,String) ={
    val tableDesc=hiveTableName.split("\\.",-1)
    if(tableDesc.length==2) {
      val db = tableDesc(0)
      val table = tableDesc(1)
      (db,table)
    }else{
      ("tmp",hiveTableName)
    }
  }



}
