package com.huoli.statictag.sqoop

import java.sql.{Connection, DriverManager, ResultSet}
import com.huoli.statictag.utils.Config
import org.apache.log4j.Logger
/**
  *  sqoop 工具使用过程中的一些公共的方法抽取出来
  *  一遍后续重复使用
  *  create date 2018-08-06
  *  auth  zengxy
  */
object SqoopUtil {


  private  val logger:Logger=Logger.getLogger(this.getClass)

  // 执行多条sql
  @throws (classOf[Exception])
  def executeSql(sqls:String*)={

    val url=Config("etl_db.url")
    val username=Config("etl_db.username")
    val password=Config("etl_db.password")

    // Change to Your Database Config
    val conn_str = s"$url&user=$username&password=$password"

    var conn:Connection=null

    var current_sql=""
    try {

      // Load the driver
      Class.forName("com.mysql.jdbc.Driver")
      // Setup the connection
      conn = DriverManager.getConnection(conn_str)
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      for(sql<-sqls){
        current_sql=sql
        logger.info(s"begin to execute sql:$conn_str ")
        statement.execute(sql)
        logger.info(s" execute sql:$conn_str  successful")
      }
    }catch{
      case e:Exception=>
        logger.info(s"when execute sql:${current_sql} throw an exception  ",e)
        throw e
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
  def insertTagTabTemp(mysqlTableName : String,hiveTableName:String,hivedb:String="tmp",mapNumber:Int=15) = {

    val tmpMysqlTableName=s"${mysqlTableName}_tmp"

    executeSql(s"truncate table ${tmpMysqlTableName}")

    val url=Config("etl_db.url")
    val username=Config("etl_db.username")
    val password=Config("etl_db.password")

    val sqoop_cmd= s"""sqoop export  -m $mapNumber
                      |--connect  $url
                      |--username $username
                      |--password $password
                      |--table    $tmpMysqlTableName
                      |-outdir /home/bigdata/data_job_workspace/sqoop_outdir
                      |--export-dir /hive/warehouse/${hivedb}.db/$hiveTableName
                      |--fields-terminated-by "\\0001"
                      |--input-null-string 'NULL'
                      |--input-null-non-string 'NULL'
    """.stripMargin
    import scala.sys.process._
    sqoop_cmd.!
  }

  /**
    *  rename mysql table
    * @param mysqlTableName
    */
  def renameTables(mysqlTableName:String,moreThan:Double=0.1){

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

      rs = statement.executeQuery(s"select count(1) from ${mysqlTableName}_tmp")
      val tmp_count = if (rs.next()) rs.getInt(1) else 0

      var result =0d


      if(count==0)
        result=1d
      else
        result=tmp_count / (count * 1.0d)

      if (result > moreThan) {
        logger.info(s"table ${mysqlTableName}   count=" + count + ",tmp_count=" + tmp_count)
        executeSql(
          s"rename table $mysqlTableName to ${mysqlTableName}_old",
          s"rename table ${mysqlTableName}_tmp to $mysqlTableName",
          s"rename table ${mysqlTableName}_old to ${mysqlTableName}_tmp"
        )
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





}
