package com.qf.bigdata.release.etl.DM

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.DW.{DWReleaseColumnsHelper, DWReleaseCustomer}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object DMReleaseCustomer {
  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  /**
    * 目标客户
    * status = "01"
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    try{
      // 导入隐式转换
      import org.apache.spark.sql.functions._
      // 设置缓存级别
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode :SaveMode = ReleaseConstant.DEF_SAVEMODE
      // 获取当天日志字段数据
      val cusomerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      // 当天数据，设置条件，根据条件进行查询，后续调用数据
      val cusomerReleaseCondition =
        (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day)

      // 填入条件
      val customerReleaseDF: Dataset[Row] = SparkHelper
        .readTableData(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,cusomerColumns)
        // 查询条件
        .where(cusomerReleaseCondition)
        .persist(storageLevel)



      println("查询结束======================结果显示")
      customerReleaseDF.show(10,false)

      import spark.implicits._
      //获取目标客户DM字段
      val customerColumn: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMReleaseCustomerColumns()
      //逻辑处理，分组
      val columns: Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")
      val DMCustomer: DataFrame = customerReleaseDF.groupBy(columns: _*).agg(
        countDistinct(ReleaseConstant.COL_RELEASE_DEVICE_NUM).alias(ReleaseConstant.COL_RELEASE_USER_COUNT),
        count(ReleaseConstant.COL_RELEASE_DEVICE_NUM).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT),
        max(ReleaseConstant.DEF_PARTITION).alias(ReleaseConstant.DEF_PARTITION)
      )
        .selectExpr(customerColumn: _*)
      // 渠道用户统计
      println("渠道用户统计===============")
      DMCustomer.show(10,false)



      //多维度统计
      val DMCustomerCubeColumn: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMReleaseCustomerCubeColumns()

      val columnCube: Seq[Column] = Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}" ,
        $"${ReleaseConstant.COL_RELEASE_GENDER}",
        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}")


      val DMCustomerCube: DataFrame = customerReleaseDF.cube(columnCube: _*).agg(
        countDistinct(lit(ReleaseConstant.COL_RELEASE_DEVICE_NUM)).alias(ReleaseConstant.COL_RELEASE_USER_COUNT),
        count(ReleaseConstant.COL_RELEASE_DEVICE_NUM).alias(ReleaseConstant.COL_RELEASE_TOTAL_COUNT),
        max(ReleaseConstant.DEF_PARTITION).alias(ReleaseConstant.DEF_PARTITION)
      )
        .selectExpr(DMCustomerCubeColumn: _*)
      println("多维统计================")
      DMCustomerCube.show(20,false)







      // 目标用户
      // SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  /**
    * 投放目标客户
    */
  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark :SparkSession = null
    try{
      // spark 配置参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "8")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
        .setAppName(appName)
        .setMaster("local[*]")
      // spark  上下文
      spark = SparkHelper.createSpark(conf)
      // 参数校验
      val timeRanges = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      for (bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex :Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 如果没有Windows下的hadoop环境变量的话，需要内部执行，自己加载，如果有了，那就算了
    //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val appName :String = "dm_release_customer_job"
    val bdp_day_begin:String ="20190909"
    val bdp_day_end:String ="20190909"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

}
