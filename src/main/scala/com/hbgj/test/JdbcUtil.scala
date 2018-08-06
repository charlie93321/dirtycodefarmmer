package com.hbgj.test

import java.io.File
import java.sql._

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
  *
  *
  *
  */
class JdbcUtil(var url:String,var username:String,password:String) {
   private val driver="com.mysql.jdbc.Driver"

   private var connection:Connection=null

   private val  logger=Logger.getLogger(classOf[JdbcUtil])

   def this(){
      this("jdbc:mysql://47.93.56.114:3306/etl","adseretl","4ee4dfgfdealK")
   }
    def init {
       Class.forName(driver)
       logger.debug("连接数据库...")
       try {
         connection = DriverManager.getConnection(url, username, password)
         logger.debug(" 获取数据库连接成功")
       } catch {
         case e:Exception =>
              logger.debug(" 获取数据库连接失败", e)
       }
    }

    init

  def release(): Unit ={
     connection.close
  }



   def executeQuery(sql:String, params:ArrayBuffer[(String,Object)], fields:Int,filename:String){



    var  pst:PreparedStatement=null
    var rs:ResultSet=null
    try {
      pst=connection.prepareStatement(sql)

      val range=0 to params.size-1
      var index=1
      for (i<-range) {
        val kv=params(i);
        val key=kv._1
        val value=kv._2
        switchSet(index,key,value,pst)
        index=index+1
      }
      rs=pst.executeQuery()

      val fieldRange=1 to fields

      val wf=new File("D:\\zxy\\tempdir\\"+filename)
      val sb:StringBuffer=new StringBuffer()
      while(rs.next()){
        for (i<-fieldRange) {
          val r=rs.getObject(i).toString
          if(i!=fields)
            sb.append(r).append("|")
          else
            sb.append(r).append("\n")
        }
      }
      FileUtils.write(wf,sb)
    } catch {
      case e:SQLException =>
          e.printStackTrace
    }finally {
      try {
        pst.close();
      } catch {
        case e:SQLException =>
           e.printStackTrace
      }
    }
  }



   def executeBach(sql:String, params:ArrayBuffer[(String,Object)], bach:Int){

    var pst:PreparedStatement =null
    try {
      pst=connection.prepareStatement(sql)

      val size=params.size/bach
      val range =0 to size-1
      val bachRange= 0 to bach-1
      for(bachIndex<-range){

        var index=1

        for (j<-bachRange) {
          val kv=params(bachIndex*bach+j)
          val  key=kv._1
          val  value=kv._2
          switchSet(index,key,value,pst)
          index=index+1
        }
        pst.addBatch()

      }

      pst.executeBatch()
    } catch {
      case e:SQLException =>
                   e.printStackTrace()
    }finally {
      try {
        pst.close();
      } catch {
        case e: SQLException =>
                  e.printStackTrace()
      }
    }
  }

  def  switchSet(index:Int,key:String,value:Object,pst:PreparedStatement): Unit ={
     key match {
       case "STRING"=>
         try {
           pst.setString(index,value.toString());
         }catch {
           case e:Exception=>
           pst.setString(index, "");
         }
       case "INT" =>
         try {
           pst.setInt(index,value.toString.toInt)
         }catch {
           case e:Exception=>
             pst.setInt(index, -111111111);
         }

       case "LONG" =>
         try {
           pst.setLong(index,value.toString().toLong);
         }catch {
           case e:Exception=>
             pst.setLong(index, -111111111L);
         }
       case "BOOLEAN" =>
         try {
           pst.setBoolean(index,value.toString().toBoolean);
         }catch {
           case e:Exception=>
             pst.setBoolean(index,false);
         }
       case "DOUBLE"=>
         try {
           pst.setDouble(index,value.toString().toDouble);
         }catch {
           case e:Exception=>
             pst.setDouble(index,-111111111d);
         }
       case "TIMESTAMP"=>
         try
           pst.setTimestamp(index, java.sql.Timestamp.valueOf(value.toString))
         catch {
           case e: Exception =>
             pst.setTimestamp(index, java.sql.Timestamp.valueOf("1976-07-01 07:55:14.0"));
         }

       case _ =>
         println(key+"is not in my play ....................");
     }
  }


}


