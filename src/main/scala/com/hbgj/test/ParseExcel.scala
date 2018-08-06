package com.hbgj.test
import java.io.{FileInputStream, IOException, InputStream}

import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.collection.mutable.ArrayBuffer

/**
  * 读取excel 数据
  *
  *
  */
object ParseExcel {


  val util=new JdbcUtil(
    "jdbc:mysql://hdp:3306/etl?useSSL=false&useUnicode=true&characterEncoding=utf-8",
    "etl","charlie")


  def main(args: Array[String]): Unit = {
    readExcel("D:\\zxy\\tempdir\\国际各航线区分表.xlsx");

    util.release



  }




  def readExcel(fileName: String): Unit = {
    var input:InputStream=null
    try {
       input = new FileInputStream(fileName)
      //建立输入流
      val wb:Workbook  =  new XSSFWorkbook(input)




      /* doSheet(wb,0,"澳新")
       doSheet(wb,1,"欧美")
       doSheet(wb,2,"东南亚")
       doSheet(wb,3,"日韩")*/

       doSheet(wb,4,"中东")

    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }finally {
      input.close()
    }
  }

  def doSheet(workbook: Workbook,sheetIndex: Int, sheetName: String): Unit ={
    val sheet = workbook.getSheetAt(sheetIndex)
    //获得第一个表单
    val rows = sheet.rowIterator //获得第一个表单的迭代器
    var rowIndex=1

   val params:ArrayBuffer[(String,Object)]=new ArrayBuffer[(String,Object)]

    while (rows.hasNext) {
      val row = rows.next()
       if(rowIndex!=1) {
         println(s"analysis row $rowIndex")
         val country=row.getCell(0)
         val city=row.getCell(1)
         val THREE_WORDS_CODE=row.getCell(3).getStringCellValue
         params.+=(("STRING",THREE_WORDS_CODE))
         params.+=(("STRING",city))
         params.+=(("STRING",country))
         params.+=(("STRING",sheetName))
       }
       rowIndex=rowIndex+1
    }
    util.executeBach("insert into tag_area values ( ?, ? , ? , ? ) ",params,4)

  }
}

/**
  * case 0 =>
  * return "numeric"
  * case 1 =>
  * return "text"
  * case 2 =>
  * return "formula"
  * case 3 =>
  * return "blank"
  * case 4 =>
  * return "boolean"
  * case 5 =>
  * return "error"
  * case _ =>
  * return "#unknown cell type (" + cellTypeCode + ")#"
  * }
  */
object CellType extends  Enumeration{
  type CellType=Value
  val  CELL_TYPE_NUMERIC = Value(0)
  val  CELL_TYPE_STRING  = Value(1)
  val  CELL_TYPE_FORMULA = Value(2)
  val  CELL_TYPE_BLANK   = Value(3)
  val  CELL_TYPE_BOOLEAN  = Value(4)
}