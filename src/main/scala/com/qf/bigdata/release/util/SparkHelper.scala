package com.qf.bigdata.release.util

import com.qf.bigdata.release.udf.QFUdf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 工具类
  */
object SparkHelper {

  // 处理日志
   val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 读取数据表
    */
  def readTableData(spark:SparkSession,tableNam:String,colNames:mutable.Seq[String])={
    val begin = System.currentTimeMillis()
    // 读取表
    val tableDF = spark.read.table(tableNam)
      .selectExpr(colNames:_*)


    tableDF
  }

  /**
    * readtable俩表join版本
    * @param spark 会话
    * @param t1 左表
    * @param t1A 左表别名
    * @param t2 右表
    * @param t2A 右表别名
    * @param usingColumns join字段（两表必须都有的字段名）
    * @param colNames (需要的字段)
    * @return
    */
  def readTableData(spark:SparkSession,t1:String,t1A:String,t2:String,t2A:String,usingColumns: Seq[String],colNames:mutable.Seq[String])={
    val begin = System.currentTimeMillis()
    // 读取表
    val df2: Dataset[Row] = spark.read.table(t2).alias(t2A)
    val df1: Dataset[Row] = spark.read.table(t1).alias(t1A)
    val tableDF: DataFrame = df1.join(df2,usingColumns,"inner")


    tableDF.selectExpr(colNames:_*)
  }

  /**
    * 写入数据表
    */
  def writeTableData(sourceDF:DataFrame,table:String,mode:SaveMode): Unit ={
    val begin = System.currentTimeMillis()
    // 写入表数据
    sourceDF.write.format("parquet").mode(mode).insertInto(table)
    println(s"table[${table}] use :${System.currentTimeMillis()-begin}-==========")
  }

  /**
    * 创建 SparkSession
    * @param sconf
    * @return
    */
  def createSpark(sconf:SparkConf):SparkSession={
    val spark = SparkSession.builder()
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate()
    // 为了处理目标主题下面的DM层统计 年龄段做准备
    registerFun(spark)
    spark
  }

  /**
    * UDF 注册
    */
  def registerFun(spark:SparkSession): Unit ={
    // 处理年龄段
    spark.udf.register("getAgeRange",QFUdf.getAgeRange _)
  }

  /**
    * 参数校验
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try {
      val bdp_date_begin = DateUtil.dateFormat4String(begin,"yyyyMMdd")
      val bdp_date_end = DateUtil.dateFormat4String(end,"yyyyMMdd")

      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while(cday != bdp_date_end){
          bdp_days.+=(cday)
          val pday = DateUtil.dateFormat4StringDiff(cday, 1)
          cday = pday
        }
      }
    }catch {
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }
    bdp_days
  }


}
