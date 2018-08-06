package com.hbgj.test
import org.apache.poi.hssf.usermodel.HSSFCell
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import java.io.FileInputStream
import java.io.IOException

import org.apache.poi.ss.usermodel.Workbook
/**
  * 读取excel 数据
  *
  *
  */
object ParseExcel {

  def main(args: Array[String]): Unit = {
    readExcel("D:\\zxy\\tempdir\\国际各航线区分表.xlsx");


  }




  def readExcel(fileName: String): Unit = {
    try {
      val input = new FileInputStream(fileName)
      //建立输入流
      val wb:Workbook  =  new XSSFWorkbook(input)
      val sheet = wb.getSheetAt(0)
      //获得第一个表单
      val rows = sheet.rowIterator //获得第一个表单的迭代器
      while ( {
        rows.hasNext
      }) {
        val row = rows.next //获得行数据
        System.out.println("Row #" + row.getRowNum) //获得行号从0开始

        val cells = row.cellIterator //获得第一行的迭代器

        while (cells.hasNext) {
          val cell = cells.next
          println("Cell #" + cell.getColumnIndex)
          cell.getCellType match { //根据cell中的类型来输出数据
            case CellType.CELL_TYPE_NUMERIC =>
                println(cell.getNumericCellValue)

            case CellType.CELL_TYPE_STRING =>
                 println(cell.getStringCellValue)

            case CellType.CELL_TYPE_BOOLEAN =>
                  println(cell.getBooleanCellValue)

            case CellType.CELL_TYPE_FORMULA =>
                  println(cell.getCellFormula)

            case _ =>
                   println("unsuported sell type")

          }
        }
      }
    } catch {
      case ex: IOException =>
        ex.printStackTrace()
    }
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