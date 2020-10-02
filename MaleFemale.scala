package com.Projjal.KPI2
//Find the number of males and females in each state from the table.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.sum

object MaleFemale {
  
  def main(args: Array[String]) ={
    
    System.setProperty("hadoop.home.dir", "C:\\Users\\Dell\\Downloads\\Compressed\\hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "C:\\Users\\Dell\\Downloads\\Compressed\\spark-2.0.2-bin-hadoop2.6/spark-warehouse")

         
        val spark = SparkSession
                .builder
                .appName("Aadhar Card")
                .master("local")
                .getOrCreate()

               
    
    val df = spark.read.csv("C:\\Users\\Dell\\Downloads\\Music\\aadhaar_data.csv")
 
    
   val x= df.filter(df("_c7")!=="T")  //To filter out gender="T"
   
   
   /* x.orderBy("_c3").groupBy("_c3", "_c7").agg(sum("_c9")).show()*/
    
   x.createOrReplaceTempView("aadhar")
   spark.sql("SET spark.sql.shuffle.partitions = 5")            //To avoid data skewing
   spark.sql("select _c3,_c7,sum(_c9) from aadhar group by _c3,_c7 order by _c3 ").show()
  
  spark.stop()
  }
}