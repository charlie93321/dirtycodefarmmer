package com.huoli.statictag.sqoop
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Date
import com.huoli.statictag.utils.Config
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Logger
import scala.sys.process._

/**
  * create date 2018-08-02
  * auth zengxy@133.cn
  */
object Sqoop_ResidentCityNew {
  private val logger:Logger=Logger.getLogger(this.getClass)
  private  val  dateFormat:FastDateFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]) {

    logger.info(" begin to export hive data to mysql  : "+dateFormat.format(new Date()))

    if(args==null || args.length<2){
      logger.info("parmas's number must be 2 ")
      return
    }
    val STATICS_DATE=args(0)
    val mysqlTableName=args(1)
    val hiveTableName=s"tmp_resident_city_$STATICS_DATE"
    insertTagTabTemp(mysqlTableName, hiveTableName)
    logger.info(" begin to rename  mysql   table  : "+dateFormat.format(new Date()))
    renameTables(mysqlTableName)
    logger.info(" programe is end  : "+dateFormat.format(new Date()))
  }

  /**
    *  rename mysql table
    * @param mysqlTableName
    */
  def renameTables(mysqlTableName:String){

    val url=Config("etl_db.url")
    val username=Config("etl_db.username")
    val password=Config("etl_db.password")
    // Change to Your Database Config
    val conn_str = s"$url&user=$username&password=$password"
    var conn:Connection =null
    try {

      // Load the driver
      Class.forName("com.mysql.jdbc.Driver")
      // Setup the connection
      conn = DriverManager.getConnection(conn_str)

      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query

      var rs = statement.executeQuery(s"select count(1) from $mysqlTableName")
      val count = if (rs.next()) rs.getInt(1) else 0

      rs = statement.executeQuery(s"select count(1) from " + mysqlTableName + "_tmp")
      val tmp_count = if (rs.next()) rs.getInt(1) else 0

      var result =0d


      if(count==0)
           result=1d
      else
           result=tmp_count / (count * 1.0d)

      /**
        * 这个0.25必须详细说明才行
        *
        * 他是假设了一种极端情况,取得了最小值
        *
        * 我们假设 用户数量 两次周期之间 没有增加
        * 原来每个用户4条数据  现在每个用户1条数据   0.25 就是这么来的
        * 当然这么多情况不可能都同时满足  故0.25边界不包含
        */
      if (result > 0.25) {
        logger.info(s"table ${mysqlTableName}   count=" + count + ",tmp_count=" + tmp_count)
        statement.execute(s"rename table $mysqlTableName to ${mysqlTableName}_old")
        statement.execute(s"rename table ${mysqlTableName}_tmp to $mysqlTableName")
        statement.execute(s"rename table ${mysqlTableName}_old to ${mysqlTableName}_tmp")
      } else {
        logger.info(s"app error:table ${mysqlTableName} rename fail,cause count gt tmp_count,count=" + count + ",tmp_count=" + tmp_count)
      }
    }catch{
        case e:Exception=>
          logger.info(s"when execute rename mysql table  throw an exception  ",e)

    }finally {
      conn.close
    }
  }

  def execute(sql:String)={

    val url=Config("etl_db.url")
    val username=Config("etl_db.username")
    val password=Config("etl_db.password")

    // Change to Your Database Config
    val conn_str = s"$url&user=$username&password=$password"

   var conn:Connection=null
    try {

      // Load the driver
      Class.forName("com.mysql.jdbc.Driver")
      // Setup the connection
      conn = DriverManager.getConnection(conn_str)
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      statement.execute(sql)
    }catch{
      case e:Exception=>
           logger.info(s"when execute sql:$sql throw an exception  ",e)
    }finally {
      conn.close
    }
  }

  /**
    *   将hive中的数据导入mysql的临时表中
    * @param mysqlTableName
    * @param hiveTableName
    * @return
    */
  def insertTagTabTemp(mysqlTableName : String,hiveTableName:String) = {

    val tmpMysqlTableName=s"${mysqlTableName}_tmp"

    execute(s"truncate table ${tmpMysqlTableName}")

    val url=Config("etl_db.url")
    val username=Config("etl_db.username")
    val password=Config("etl_db.password")

   val sqoop_cmd= s"""sqoop export  -m 15
      |--connect  $url
      |--username $username
      |--password $password
      |--table    $tmpMysqlTableName
      |-outdir /home/bigdata/data_job_workspace/sqoop_outdir
      |--export-dir /hive/warehouse/tmp.db/$hiveTableName
      |--fields-terminated-by "\\0001"
      |--input-null-string 'NULL'
      |--input-null-non-string 'NULL'
    """.stripMargin
    //logger.info(s"sqoop cmd=>$sqoop_cmd")
    sqoop_cmd.!



  }


}

