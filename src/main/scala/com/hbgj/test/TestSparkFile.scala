package com.hbgj.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSparkFile {
   def main(args:Array[String]): Unit ={
     val conf=new  SparkConf()
     conf.setMaster("local[20]").setAppName("test-count")
     val sc=new SparkContext(conf)

     val fileRdd=sc.textFile("D:\\zxy\\tempdir\\travel_count_20180806")


     fileRdd.map(line=>{
         val arr=line.split("\t")
       if(arr.length!=2)println("error message=========================>"+line)
         val num=arr(1).toInt
          if(num<5){
            ("0~5",1)
          }else if(num<10){
            ("5~10",1)

          }else if(num<15){
            ("10~15",1)

          }else if(num<20){
            ("15~20",1)

          }else if(num<25){
            ("20~25",1)

          }else if(num<30){
            ("25~30",1)

          }else if(num<40){
            ("30~40",1)

          }else if(num<50){
            ("40~50",1)

          }else if(num<60){

            ("50~60",1)
          }else if(num<70){

            ("60~70",1)
          }else if(num<80){

            ("70~80",1)
          }else if(num<90){

            ("80~90",1)
          }else if(num<100){
            ("90~100",1)

          }else{
            ("gt100",1)
          }

     }).reduceByKey((a,b)=>a+b).saveAsTextFile("D:\\zxy\\tempdir\\travel_count_result3")

     sc.stop()
   }
}
